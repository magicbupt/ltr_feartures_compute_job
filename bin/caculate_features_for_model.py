#!/usr/bin/env python

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import math
from ConfigParser import RawConfigParser
from ConfigParser import *

sys.path.append('../lib')
sys.path.append('lib')
import logging 
import logging.config
from mail_sender import MailSender

class CaculateFeatures4Model():
	def __init__(self, conf_path = '..', feature_num = 18):
		self.feature_num = feature_num
		#other config such as email recivers
		self.filecfg = '%s/conf/main.cfg' % (conf_path)
		self.fileconfig = RawConfigParser()
		self.fileconfig.read(self.filecfg)
		# config email info
		self.mail_recivers = self.setMailReceiver('email', 'receiver')
		self.mail_sender = MailSender()

		self.feature_index = {}
		self.classify_index = {}
		self.weight_index = {}
		self.weight_value = {}
		self.product_info = {}
		self.product_norm_info = {}
		self.setFeatureAndClassifyIndex()

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

	def setFeatureAndClassifyIndex(self, section="index"):
		feature_list = self.fileconfig.get(section,"feature_index").strip().split(',')
		lenght = len(feature_list)
		for i in range(lenght):
			array = feature_list[i].strip().split(':')
			feature_index = int(array[0])
			feature_name = array[1].strip()
			self.feature_index[feature_index] = feature_name

		classify_list = self.fileconfig.get(section,"classify_index").split(',')
		lenght = len(classify_list)
		for i in range(lenght):
			array = classify_list[i].strip().split(':')
			classify_index = int(array[0])
			classify_name = array[1].strip()
			self.classify_index[classify_name] = classify_index

		weight_list = self.fileconfig.get(section,"weight_index").split(',')
		lenght = len(weight_list)
		for i in range(lenght):
			array = weight_list[i].strip().split(':')
			weight_index = int(array[0])
			weight_name = array[1].strip()
			self.weight_index[weight_index] = weight_name

	def initWeights(self, filepath):
		file = None
		try:
			k = 0
			file = open(filepath, 'r')
			while True:
				line = file.readline()
				if not line:
					break
				item_name = self.weight_index[k]
			#	print item_name
				self.weight_value[item_name] = float(line.strip())
				k += 1
		except Exception,ex:
			self.logger.error(ex)
			self.mail_sender.sendMail(self.mail_recivers)
			raise Exception, ex		
		finally:
			if file:
				file.close()

	def normalizedFeature(self, val, max_val = 2209, is_log = True):
		if val > max_val:
			return 1
		if val <= 0:
			return 0

		if is_log:
			val = math.log(val + 1)
			max_val = math.log(max_val + 1)
		return val/max_val

	def loadLogData(self, srcfilepath, disfilepath):
		srcfile = None
		disfile = None
		try:
			srcfile = open(srcfilepath, "r")
			disfile = open(disfilepath, "w+")
			default_ctr = 1.0/(1 + math.exp(- self.weight_value['bias']))
			disfile.write("%s\t%s\n" % (-1, default_ctr))
			while True:
				line = srcfile.readline()
				if not line:
					break
				array = line.strip("\n").split("\t")
				lenght = len(array)
				if lenght != self.feature_num:
					continue

				pid = str2int(array[0])
				pid_feature = {}
				for i in range(1, self.feature_num):
					item_name = self.feature_index[i - 1]
					val = round(str2float(array[i]), 3)
					pid_feature[item_name] = val
			
				pid_process_feature = self.featureProcess(pid_feature)
				pid, ctr = self.estimateCTR(pid, pid_process_feature)
		#		self.product_info[pid] = pid_feature
		#		self.product_norm_info[pid] = pid_process_feature
						
				disfile.write("%s\t%s\n" % (pid, ctr))
		except Exception,ex:
			self.logger.error(ex)
			print 'Can not connect to dbserver'
			self.mail_sender.sendMail(self.mail_recivers)
			raise Exception, ex
		finally:
			if srcfile:
				srcfile.close()
			if disfile:
				disfile.close()

	def featureProcess(self, pid_feature):	
		pid_processed_feature = {}

		is_new = str2int(pid_feature['is_new'])
		pid_processed_feature['is_new'] = is_new		

		is_promo = str2int(pid_feature['is_discount'])
		pid_processed_feature['is_discount'] = is_promo

		score = str2float(pid_feature['score'])
		if score >= 8:
			score = 1
		else:
			score = 0
		pid_processed_feature['score'] = score

		d_uv = self.normalizedFeature(pid_feature['d_uv'], 50, False)
		pid_processed_feature['d_uv'] = d_uv

		w_uv = self.normalizedFeature(pid_feature['w_uv'])
		pid_processed_feature['w_uv'] = w_uv

		m_uv = self.normalizedFeature(pid_feature['m_uv'])
		pid_processed_feature['m_uv'] = m_uv

		n_comm = self.normalizedFeature(pid_feature['review_total'])
		pid_processed_feature['review_total'] = n_comm

		n_keep = self.normalizedFeature(pid_feature['n_keep'])		
		pid_processed_feature['n_keep'] = n_keep

		return pid_processed_feature


	def estimateCTR(self, pid, pid_feature_dt):
		ctr = self.weight_value['bias']
		for k,w in self.weight_value.items():
			if k == 'bias':
				continue
			feature_val = 0
			if k in pid_feature_dt:
				feature_val = pid_feature_dt[k]
			ctr += w*feature_val
		return pid, 1.0/(1 + math.exp(0 - ctr))

	def run(self, weightpath, srcfilepath, disfilepath):
		self.initWeights(weightpath)
		#print self.weight_value
		self.loadLogData(srcfilepath, disfilepath)
		
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
	if len(sys.argv) != 2:
		return
	curday = sys.argv[1]
	weightpath = "../data/model.txt"
	srcfilepath = "../data/product_info_%s" % curday
	disfilepath = "../data/pid_ctr"
	o = CaculateFeatures4Model()
	o.run(weightpath, srcfilepath, disfilepath)

if __name__ == "__main__":
	main()
