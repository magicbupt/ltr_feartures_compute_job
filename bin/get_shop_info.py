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

class ShopInfo(object):
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
		self.conn_shop_db = self.connDbserver('com_shop_review')
		self.sql = self.sqlfig.get('select', 'select_shop_info')	
		# config email info
		self.mail_recivers = self.setMailReceiver('email', 'receiver')
		self.mail_sender = MailSender()

		#dict
		self.shopid_info_score = {}
		self.shopid_price_score = {}
		self.shopid_payment_score = {}
		self.shopid_deliver_score = {}
		self.shopid_package_score = {}
		self.shopid_average_score = {}
		
		#shop average score
		self.shopid_scores = {}
		

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

	def connDbserver(self, section):
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

	def getShopInfo(self, curday):
		sql = self.sql % curday
		try:
			self.conn_shop_db.execute(sql)
			while True:
				line = self.conn_shop_db.fetchmany(10000)
				if not line:
					break
				for i in range(len(line)):
					array = line[i]
					shopid = array[0]
					self.addInfoScore(shopid, array[1])
					self.addPriceScore(shopid, array[2])
					self.addPaymentScore(shopid, array[3])
					self.addDeliverScore(shopid, array[4])
					self.addPackageScore(shopid, array[5])
					self.addAverageScore(shopid, array[6])					
		except Exception,ex:
			print ex
			self.logger.error(ex)
			self.mail_sender.sendMail(self.mail_recivers)
		finally:
			if self.conn_shop_db:
				self.conn_shop_db.close()
	
	def addInfoScore(self, shopid, score):
		self.addScore(self.shopid_info_score, shopid, score)

	def addPriceScore(self, shopid, score):
		self.addScore(self.shopid_price_score, shopid, score)

	def addPaymentScore(self, shopid, score):
		self.addScore(self.shopid_payment_score, shopid, score)

	def addDeliverScore(self, shopid, score):
		self.addScore(self.shopid_deliver_score, shopid, score)

	def addPackageScore(self, shopid, score):
		self.addScore(self.shopid_package_score, shopid, score)

	def addAverageScore(self, shopid, score):
		self.addScore(self.shopid_average_score, shopid, score)

	def addScore(self, dt, shopid, score):
		if dt == None:
			dt = {}
		
		if shopid not in dt:
			dt[shopid] = [0 for i in range(6)]
		
		if score == None or score < 0:
			score = 0
		if score > 5:
			score = 5
		
		score = int(score)		
		dt[shopid][score] += 1
				

	def updataShopAllScore(self):
		'''
		caculate all item socre's expectation for each shopid
		'''
		self.updateShopEachScores(self.shopid_info_score, "info")
		self.updateShopEachScores(self.shopid_price_score, "price")
		self.updateShopEachScores(self.shopid_payment_score, "payment")
		self.updateShopEachScores(self.shopid_deliver_score, "deliver")
		self.updateShopEachScores(self.shopid_package_score, "package")
		self.updateShopEachScores(self.shopid_average_score, "average")		

	def updateShopEachScores(self, dt, index):
		'''
		caculate each score_item's expectation for each shopid 
		'''
		E_score = 0
		n_sum = 0
		for shopid, s_info in dt.items():
			E_score = 0
			n_sum = 0
			for i in range(len(s_info)):
				n_sum += s_info[i]
				E_score += i*s_info[i]
			
			if n_sum == 0:
				E_score = 0
			else:
				E_score = E_score/(1.0*n_sum) 
			
			R = 3.0
			c = 30
			E_score = (n_sum*E_score + c*R)/(n_sum + c)
			E_score	= round(E_score, 2)	
	
			if shopid not in self.shopid_scores:
				self.shopid_scores[shopid] = {}

			self.shopid_scores[shopid][index] = E_score

	def saveShopScore(self, filepath):
		file = None
		try:
			file = open(filepath, "w+")
			for shopid, scores in self.shopid_scores.items():
				print shopid, scores
				file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (shopid, scores['info'], \
					scores['price'],scores['payment'], scores['deliver'],\
					scores['package'], scores['average']))
		except Exception,ex:
			print ex
		finally:
			if file:
				file.close()

	def run(self, filepath):	
		'''
		
		'''
		curday = datetime.datetime.now() + datetime.timedelta(days=-30)
		curday = curday.strftime('%Y-%m-%d')		
		print "i am here 111"	
		self.getShopInfo(curday)
		print "i am here 2222"	
		self.updataShopAllScore()
		print "i am here 333"	
		self.saveShopScore(filepath)
		print "i am here 444"	
		

def main():
	print "go ..."
	o = ShopInfo()

	print "run ..."
	filepath = "../data/shop_score.txt"
	o.run(filepath)

	print "end !"

if __name__ == "__main__":
	main()















