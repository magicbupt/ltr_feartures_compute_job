#!/usr/bin/env python
#encoding:utf-8

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
import re
import time, datetime
import math
from ConfigParser import RawConfigParser
from ConfigParser import *

sys.path.append('../lib')
import logging
import logging.config
from mail_sender import MailSender
from data_access_object import DataObject
from db_factory import DBFactory


class ProductInfo(object):
	def __init__(self, conf_path = '..'):
		'''
		init
		'''
		#db redis config file position
		self.conncfg = '%s/conf/connect.cfg' % (conf_path)
		#log config file
		self.logcfg = '%s/conf/logger.conf' % (conf_path)
		logging.config.fileConfig(self.logcfg)
		self.logger = logging.getLogger('log')
		#other config such as email recivers
		self.filecfg = '%s/conf/main.cfg' % (conf_path)
		self.fileconfig = RawConfigParser()
		self.fileconfig.read(self.filecfg)
		#sql config file
		self.sqlcfg = '%s/conf/sql.cfg' % (conf_path)
		self.config = RawConfigParser()
		self.config.read(self.conncfg)
		self.sqlfig = RawConfigParser()
		self.sqlfig.read(self.sqlcfg)
		#connect db redis
		self.conn_search_db = self.connDbserver('search_v3_view')
#		self.conn_redis = self.connRedis()
		self.sql = self.sqlfig.get('select', 'select_product_info')
		# config email info
		self.mail_recivers = self.setMailReceiver('email', 'receiver')
		self.mail_sender = MailSender()
		#product info
		self.pid_info = {}
		self.pid_day_uv = {}
		self.pid_week_uv = {}
		self.pid_month_uv = {}


	def readUV(self, filepath, dt):
		file = None
		try:
			file = open(filepath, 'r')
			while True:
				line = file.readline()
				if not line:
					break
				array = line.strip().split()
				if len(array) != 2:
					continue
				pid = str2int(array[0])
				uv = str2int(array[1])
				dt[pid] = uv
		except Exception,ex:
			print ex
		finally:
			if file:
				file.close()

	def getUV(self, pid, dt):
		if pid in dt:
			return dt[pid]
		return 0
		
	def setMailReceiver(self,section = 'email', part = 'receiver'):
		email_list = self.fileconfig.get(section,part).split(',')
		mail_receiver = []
		lenght = len(email_list)
		for i in range(lenght):
			email = email_list[i].strip(' ')
			if email == '':
				continue
			mail_receiver.append(email)
		return mail_receiver

	def connDbserver(self, section = 'antifraud_conn'):
		dbserver = None
		try:
			dbtype = self.config.get(section, 'dbtype')
			host = self.config.get(section, 'host')
			port = self.config.get(section, 'port')
			user = self.config.get(section, 'user')
			password = self.config.get(section, 'password')
			database = self.config.get(section, 'database')
			dbserver = DBFactory.Connect(dbtype = dbtype, host = host, database = database, \
					charset = 'utf8',user = user, password = password, port = port)
		except Exception, ex:
			self.logger.error(ex)
			print 'Can not connect to dbserver'
			self.mail_sender.sendMail(self.mail_recivers)
			raise Exception, ex
		return dbserver

	def getProductInfo(self, filepath):
		sql = self.sql
		file = None
		try:
			self.conn_search_db.execute(sql)
			file = open(filepath, 'w+')
			while True:
				line = self.conn_search_db.fetchmany(10000)
				if not line:
					break
				for i in range(len(line)):
					array = line[i]
					if len(array) != 16:
						continue
					pid, is_new, is_mall, is_discount, score, discount, \
						sale_day, n_sale, sale_month, n_keep, \
						review_total, price, dd_price, \
						promo_price, n_days = self.caculateModel(array)
					d_uv = self.getUV(pid, self.pid_day_uv)
					w_uv = self.getUV(pid, self.pid_week_uv)
					m_uv = self.getUV(pid, self.pid_month_uv)
					product_info = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (is_mall, is_new, \
						is_discount, score, discount, sale_day, n_sale, sale_month, n_keep, review_total, price, dd_price,\
						promo_price, d_uv, w_uv, m_uv, n_days)
					file.write("%s\t%s\n" % (pid, product_info))
		except Exception,ex:
			print ex
			self.logger.error(ex)
			self.mail_sender.sendMail(self.mail_recivers)
		finally:
			if file:
				file.close()
			if self.conn_search_db:
				self.conn_search_db.close()
	
	
	def readProductInfo(self, srcpath, n_features):
		file = None
		try:
			file = open(srcpath, 'r')
			while True:
				line = file.readline()
				if not line:
					break
				array = line.strip("\n").split("\t")
				lenght = len(array)
				if lenght != n_features:
					continue
				pid = str2int(array[0])
				product_info = ""
				for i in range(1, lenght):
					product_info = "%s\t%s" % (product_info, array[i])
				product_info = product_info.strip("\t")
				self.pid_info[pid] = product_info
		except Exception,ex:
			print ex
		finally:
			if file:
				file.close()		
	
	def IsNewProduct(self, pid, first_input_date, cat_paths):
		if first_input_date==None or str(first_input_date)=='':
			return 0, 0
		if cat_paths == None:
			cat_paths = "01"
		
		mall_new_days = 14
		pub_new_days = 30
		mall_time = ( datetime.datetime.now() + \
				datetime.timedelta(days=-mall_new_days) ).strftime('%Y-%m-%d')
		pub_time = ( datetime.datetime.now() + \
				datetime.timedelta(days=-pub_new_days) ).strftime('%Y-%m-%d')

		mall_new = int(time.mktime(time.strptime(mall_time, '%Y-%m-%d')))
		pub_new = int(time.mktime(time.strptime(pub_time, '%Y-%m-%d')))
		first_date = int(time.mktime(\
				time.strptime(first_input_date.strftime('%Y-%m-%d'), '%Y-%m-%d')))

		cat = cat_paths.strip().split('|')[0].split('.')[0]
		
		is_mall_product = 0
		if cat == "58":
			is_mall_product = 1
		
		if cat=="58" and (first_date-mall_new)>0:
			return 1, is_mall_product
		elif cat=="01" and (first_date-pub_new)>0:
			return 1, is_mall_product
		else:
			return 0, is_mall_product
		return 0, is_mall_product

	def IsDiscountProduct(self, begin_date, end_date):
		if begin_date != None:
			now_date = datetime.datetime.now()
			if now_date >= begin_date:
				if end_date != None:
					if now_date <= end_date:
						return 1
				else:
					return 1
		return 0

 	def calDiscount(self, price, dd_price, promo_price, isDiscount):
		discount = 0
		price = str2float(price)
		dd_price = str2float(dd_price)
		promo_price = str2float(promo_price)

		if price == 0:
			discount = 10
		else:
			if isDiscount == 1 and promo_price != 0:
				discount = promo_price/(1.0*price)*10
			elif dd_price != 0:
				discount = dd_price/(1.0*price)*10
			else:
				discount = 0

		return round(discount, 2), price, dd_price, promo_price

	def productScore(self, score, total_review_count):
		if score == None:
			score = 0
		if total_review_count == None:
			total_review_count = 0
		m = 100
		average_score = 5.0
		val = (total_review_count*score + m*average_score)/(1.0*(total_review_count + m))
		return val

	def calSaledDays(self, input_day):
		if input_day == None:
			return 1000
		nowday = datetime.datetime.now()
		delta = nowday - input_day
		return delta.days	

	def caculateModel(self, array):
		pid = str2int(array[0]) # get pid		
		is_new, is_mall = self.IsNewProduct(pid, array[7], array[8]) #is new
		is_discount = self.IsDiscountProduct(array[5], array[6])  #is discount
		discount, price, dd_price, promo_price = self.calDiscount(array[10], \
					array[11], array[12], is_discount) # discount
		n_keep = str2int(array[4]) #keep num
		n_sale = str2int(array[1]) #sale num
		score = str2float(array[3])  # product score
		review_total = str2int(array[2]);
		sale_day = str2int(array[13]);
		sale_month = str2int(array[14]);
		n_days = self.calSaledDays(array[15])
		
		return pid, is_new, is_mall, is_discount, score, discount, sale_day,\
				 n_sale, sale_month, n_keep, review_total, price, dd_price, promo_price, n_days;

	def run(self, search_log_file = "../data/product_info_", n_features = 18):
		try:
			curday = datetime.datetime.now().strftime('%Y-%m-%d')
			curday = getSpecifiedDay(curday, 1)
			
			search_log_file = "%s%s" % (search_log_file, curday.replace("-", ""))
			if not os.path.exists(search_log_file):
				self.readUV("../data/temp_data/uv_d_%s" % curday, self.pid_day_uv)
				self.readUV("../data/temp_data/uv_w_%s" % curday, self.pid_week_uv)
				self.readUV("../data/temp_data/uv_m_%s" % curday, self.pid_month_uv)
				self.getProductInfo(search_log_file)
			self.readProductInfo(search_log_file,n_features)
		except Exception,ex:
			self.logger.error(ex)
			self.mail_sender.sendMail(self.mail_recivers)	


def getSpecifiedDay(curday, n):
	'''
	get the n days ago
	'''
	try:
		array = curday.split('-')
		if len(array) != 3:
			return
		year = int(array[0])
		month = int(array[1])
		day = int(array[2])
		preday = datetime.datetime(year, month, day) - datetime.timedelta(n)
		return  preday.strftime("%Y-%m-%d")
	except Exception ,ex:
		print 'get preday error~~'
		raise ex
	return

def str2float(f_str):
	val = 0
	try:
		val = float(f_str)
		if val < 0:
			val = 0
	except:
		val = 0
	return val
			
def str2int(i_str):
	val = 0
	try:
		val = int(i_str)
		if val < 0:
			val = 0
	except:
		val = 0
	return val


def main():
	o = ProductInfo()
	o.run()

if __name__=="__main__":
	main()


