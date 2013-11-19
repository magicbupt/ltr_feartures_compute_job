#!/usr/bin/env python
#encoding:utf-8

##Author:maorui
##Date  :2012-08-30
##File  :base_lib.py
##comment:基础类库用于封装一些简单的功能
##		
		
import sys
import os
from ConfigParser import RawConfigParser
from ConfigParser import *
from getopt import getopt
import logging
import logging.config
import time
import datetime
import socket				
import struct

from data_access_object import DataObject
from db_factory import DBFactory
from Joblog import Joblog
from mail_sender import MailSender

def get_path():
	
	"""
	get opt parameter
	"""
	path =''
	try:
		opts, val = getopt(sys.argv[1:], 'c:', ['path='])
		conf_path = ''
		for opt, var in opts:
			if opt in ['-c']:
				conf_path = var
			if opt in ['--path']:
				conf_path = var
		if not os.path.exists(conf_path):
			raise Exception,'the path is not existence'
	except Exception,ex:
		raise Exception,ex

	return conf_path   



class BaseLib(object):

	def __init__(self,conf_path):
	   
		'''
		初始化配置文件和日志文件
		'''
		self.connfile = '%s/conf/connect.cfg' % (conf_path)
		self.sqlfile = '%s/conf/sql.cfg' % (conf_path)
		
		self.config = RawConfigParser()
		self.config.read(self.connfile)
		self.sqlsets = RawConfigParser()
		self.sqlsets.read(self.sqlfile)
	
	def connect_db(self,section = ''):
		"""
		主要用于连接数据库，并返回连接对象
		"""
		section = section.strip(' ')
		try:
			dbtype = self.config.get(section,'dbtype')
			host = self.config.get(section,'host')
			user = self.config.get(section,'user')
			password = self.config.get(section,'password')
			port = self.config.get(section,'port')
			database = self.config.get(section,'database')
		except: 
			raise Exception, 'get %s date error' % (section)
		try:
			sql_conn = DBFactory.Connect(dbtype = dbtype,host = host,database = database,charset = 'utf8',user = user,\
							 password = password,port = port)
			sql_conn.get_cursor()
		except:
			raise Exception, 'connect databse under %s error' % (section)
		return sql_conn
	
	def get_sql(self,section = '',option = ''):
		'''
		获取配置文件中的sql语句
		'''
		sql = ''
		section = section.strip(' ')
		option = option.strip('')
		try:
			sql = self.sqlsets.get(section, option)
		except: 
			raise Exception, 'get sql under %s %s error' % (section,option)
		return sql

	def get_monitor(self,title = ''):
		"""
		get monitor object
		"""
		joblog = None
		title = title.strip(' ')

		try: 
			host   = self.config.get(title, 'host')
			uid	= self.config.get(title, 'user')
			pwd	= self.config.get(title, 'password')
			db	 = self.config.get(title, 'database')
			joblog = Joblog(host, uid, pwd, db)
			
		except Exception, ex:  
			return 

		return joblog

	def send_mail(self,title, content):
		'''
		出现异常是发送邮件报告
		'''
		#初始化邮件发送对象

		email = MailSender()
		try:
			email_to = self.config.get('email','receiver')
			email_receivers = email_to.split(';')
			email.sendMail(email_receivers,title, str(content))
		except Exception, ex: 
			return 




def test():
	path = get_path()
	print path

	obj = BaseLib(path)
	
	sql_conn = obj.connect_db('connect_mysql')
	sql = obj.get_sql('SELECT','get_region_list')
	sql_conn.execute(sql)

if __name__ == '__main__':
	test()
