#!/usr/bin/env python
#encoding:utf8

## Author : xiaominggege
## Modited: liuxufeng
## last change time: 2012-08-21
## work for: 发送错误邮件，邮件内容包含了traceback信息
## 不支持：
##	   附件，抄送

import os
import sys
import traceback
import inspect
import datetime
import smtplib, mimetypes
from email.MIMEText import MIMEText


class MailSender(object):
	'''
	向邮箱发送错误信息
	用法：MailSender().SendMail(receiverlist, sub = 'AntiFraud ERROR', content= 'Error info')
	'''
	def __init__(self):
		self.frm = "ddclick_download_script@dangdang.com"
		self.message = ''  # restore the info of traceback
		self.errFilePath = ''
	
	def write(self, str):
		'''
		把traceback信息存储必须的函数
		'''
		self.message += str

	def __getContent(self):
		'''
		得到traceback信息
		'''
		traceback.print_exc(file = self)

	def __getReceiverList(self, receivers) :
		'''
		得到收件人列表
		'''
		if isinstance(receivers, list):
			return ';'.join(receivers)
		return receivers

	def __getErrFilePath(self):
		'''
		得到发生error的文件的路径
		'''
		n = len(inspect.stack())
		current_file = inspect.stack()[n-1][1]
		return os.path.abspath(current_file)
	
	def sendMail(self, receiver, sub = 'AntiFraud ERROR', content= 'Error info'):
		'''
		发送邮件
		'''
		receiverlist = self.__getReceiverList(receiver)
		self.__getContent()
		self.errFilePath = self.__getErrFilePath()
		content = ">>>%s<<<%s%s%s%s" % (content, '\nin: ',str(self.errFilePath), '\n', self.message)
		try:
			msg = MIMEText(content)
			msg['From'] = self.frm
			msg['To'] = receiverlist
			msg['Subject'] = sub
			smtp_server = smtplib.SMTP('localhost')
			smtp_server.sendmail(self.frm, receiver, msg.as_string())
			smtp_server.quit()
		except Exception, ex:
			print 'Error when sending email'
			raise ex
	#

def test():
	receiver = ['516269695@qq.com']
	mm = MailSender()
	try:
		a = int('')
	except Exception, ex:
		#sub = 'Error'
		#content = 'ERror when program running'
		print ex
		mm.sendMail(receiver)

	
if __name__ =='__main__':
	test()

