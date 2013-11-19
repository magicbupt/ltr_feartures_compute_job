#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
   Author:yuecong
   UploadTime:2012-07-22 17:07:00
'''

import re
import sys
import ConfigParser
#sys.path.append('/usr/local/hive')

#from hive_service import ThriftHive
#from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
 
class DataObject(object):
	def __init__(self):
		self.conn_str = []
		self.conn = None
		self.cursor = None
		self.rowcount = 0
		self.config = None
		self.tmpfile = ''
		self.debug_mode = False 
		reload(sys)
		sys.setdefaultencoding('utf8')


	def connect(self,dbtype, host = '', dsn = '', database = '', charset = '', user = '', password = '', port = 0):
		'''
			连接指定的数据源，MySQL,SQLServer,Oracle,Hive
			params:
			dbtype: 'mysql', 'mssql', 'oracle', 'hive'
			host: 域名或ip地址
			dsn: 连接oracle数据库专用
			charset: 如果原生的驱动支持charset选项，则可以设置，MySQLdb, pymssql支持， cx_Oracle与HIveThrift不支持
		'''
		self.conn_str = [dbtype,host,user,password,database,port,charset]

		_dbtype = dbtype.lower()
		_host = host
		_dsn = dsn
		_user = user
		_password = password
		_db = database
		_port = int(port)
		_charset = charset

		if _dbtype == 'mysql':
			import MySQLdb
			self.conn = MySQLdb.connect(host = _host, db = _db, port = _port, charset = _charset, \
								   user = _user, passwd = _password, use_unicode = 'True')
		if _dbtype == 'mssql':
			import pymssql
			self.conn = pymssql.connect(host = _host, user = _user, password = _password, database = _db, charset = _charset) 

		if _dbtype == 'oracle':
			import cx_Oracle
			self.conn = cx_Oracle.connect(user = _user, password = _password, dsn = _dsn)

		if _dbtype == 'hive':
			try:
				transport = TSocket.TSocket(host = _host, port = _port)
				self.conn = TTransport.TBufferedTransport(transport)
				self.conn.open()
			except Thrift.TException, tx:
				sys.stderr.write('Hive Connection Error:%s\n' % (tx))
				self.close()
				raise SystemError, tx
		return self


	def Connect(dbtype, host = '', dsn = '', database = '', charset = '', user = '', password = '', port = 0):
		'''
			connect函数的静态版
			返回一个DataObject连接对象
		'''
		_dbtype = dbtype.lower()
		_host = host
		_dsn = dsn
		_user = user
		_password = password
		_db = database
		_port = int(port)
		_charset = charset

		obj = DataObject()
		return obj.connect(_dbtype, _host, _dsn, _db, _charset, _user, _password, _port)
	Connect = staticmethod(Connect)


	def get_origin_conn(self):
		'''
			返回原生的连接对象，从而可以使用不同具体驱动的特性
			例如 mysql连接的DataObject对象将返回 MySQLdb对象，从而
			可以使用MySQLdb.connection的成员函数（方法）
		'''
		return self.conn


	def get_cursor(self):
		'''
			[deplicate function]
			在连接上获取一个cursor,最好不要直接调用
		'''
		if self.cursor:
			self.close_cursor()

		if self.conn_str[0] in ['mssql','oracle']:
			self.cursor = self.conn.cursor()

		if self.conn_str[0] in ['mysql']:
			import MySQLdb
			self.cursor = self.conn.cursor(cursorclass=MySQLdb.cursors.SSCursor)

		if self.conn_str[0] == 'hive':
			try:
				protocol = TBinaryProtocol.TBinaryProtocol(self.conn)
				self.cursor = ThriftHive.Client(protocol)
			except Thrift.TException, ex:
				self.close()
				sys.stderr.write('%s\n' % (ex))
				raise SystemError, ex
		return self.cursor


	def close_cursor(self):
		'''
			[deplicate function]
			关闭此连接对象上的cursor
		'''
		if not self.cursor:
			return
		try:
			if self.conn_str[0] in ['mysql','mssql','oracle']:
				self.cursor.close()
				self.cursor = None
			if self.conn_str[0] == 'hive':
				self.cursor.clean()
		except Exception,ex:
			print ex 

	def _execute(self,cursor,sql):
		# 每次execute执行之前，将上次缓存的游标影响行数清空
		self.rowcount = 0
		if self.conn_str[0] in ['mysql','mssql','oracle']:
			try:
				cursor.execute(sql)
			except Exception, ex:
				sys.stderr.write('SQL or Interface Error:%s\n[SQL]:%s\n' % (ex,sql))
				raise SystemError, 'SQL or Interface Error:%s\n[SQL]:%s\n' % (ex,sql)
		if self.conn_str[0] == 'hive':
			try:
				cursor.execute(sql)
			except Thrift.TException, ex:
				sys.stderr.write('SQL or Interface Error:%s\n[SQL]:%s\n' % (ex,sql))
				raise SystemError, 'SQL or Interface Error:%s\n[SQL]:%s\n' % (ex,sql)


	def _commit(self):
		self.conn.commit()


	def execute(self,sql,params={}):
		'''
			在连接上执行一个query，params是一个字典，用params中的值替换sql中的键
		'''
		if not isinstance(params,dict):
			sys.stderr.write("Params Error:'%s' is not dictionary\n" % (params))
			raise SystemError, "Params Error:'%s' is not dictionary\n" % (params)
		#如果有参数，进行参数替换
		if len(params) != 0:
			sql = DataObject.Check_query(sql,params)
		self.get_cursor()
		if self.cursor == None:
			raise NameError, 'self.cursor is not defined!\n'
		self._execute(self.cursor, sql)


	def commit(self):
		'''
			在非自动提交模式
			针对所有update, delete, insert更新操作（execute调用）
			执行一个提交操作
			只对事务性数据库有效
		'''
		self._commit()


	def rollback(self):
		'''
			在非自动提交模式
			执行一个回滚操作
			对事务性数据库有效
		'''
		if self.conn_str[0] in ['mysql', 'mssql', 'oracle']:
			self.conn.rollback()
		else:
			raise NotSupportedError, 'hive does not support rollback'


	def Get_query(configpath,title,sqlname,params = {}):
		'''
			get_query的静态版本
		'''
		if not isinstance(params,dict):
			sys.stderr.write("Params Error:'%s' is not dictionary\n" % (params))
			raise SystemError, "Params Error:'%s' is not dictionary\n" % (params)
		config = ConfigParser.ConfigParser()
		config.read(configpath)
		sql = config.get(title,sqlname)
		sql = DataObject.Check_query(sql,params)
		sql = '\x20'.join(sql.split('\n'))
		return sql
	Get_query = staticmethod(Get_query)
	

	def get_query(self,configpath,title,sqlname,params = {}):
		'''
			从配置文件中获取一个sql，params是一个字典，在用值替换SQL中的键
		'''
		if not isinstance(params,dict):
			sys.stderr.write("Params Error:'%s' is not dictionary\n" % (params))
			raise SystemError, "Params Error:'%s' is not dictionary\n" % (params)
		config = ConfigParser.ConfigParser()
		config.read(configpath)
		sql = config.get(title,sqlname)
		sql = DataObject.Check_query(sql,params)
		sql = '\x20'.join(sql.split('\n'))
		return sql 


	def create_table(self,sql):
		'''
			按照SQL创建一个表
		'''
		if self.conn_str[0] in ['mysql']:
			self._create_mysql_tbl(sql)

		if self.conn_str[0] in ['hive']:
			self._create_hive_tbl(sql)
				

	def _create_hive_tbl(self,sql):
		self.execute(sql)


	def _create_mysql_tbl(self,sql):
		self.execute(sql)


	def drop_table(self,sql):
		'''
			按照SQLdrop一个表
		'''
		if self.conn_str[0] in ['mysql']:
			self._drop_mysql_tbl(sql)
		if self.conn_str[0] in ['hive']:
			self._drop_hive_tbl(sql)


	def _drop_hive_tbl(self,sql):
		self.execute(sql)


	def _drop_mysql_tbl(self,sql):
		self.execute(sql)


	def load_to_txt(self,sql,filepath,append=False, use_filter=True):
		'''
			把数据源中的数据按照SQL放入指定路径的文件中
			append: 写入文件时是否使用追加的方式，True表示使用追加
			use_filter: 是否过滤数据，True表示使用内部函数data_filter过滤数据，
						此选项将对性能造成较大影响
		'''
		if self.conn_str[0] in ['mysql','mssql','oracle']:
			self._db_to_file(sql,filepath,append, use_filter)
			return self.rowcount 

		if self.conn_str[0] in ['hive']:
			self._hive_to_file(sql,filepath,append, use_filter)
			return self.rowcount


	def _hive_to_file(self,sql,filepath,append, use_filter):
		file = None
		if append == False:
			file = open(filepath,'w')
		else:
			file = open(filepath,'a')
		self.execute(sql)
		line = ''
		while (1):
			rows = self.fetchmany(1000)
			if len(rows) == 0:
				break
			for row in rows:
				fields = []
				for item in row:
					#过滤字段中的特殊字符
					if use_filter:
					   fields.append(self.data_filter(item))
					#不过滤
					else:
					   fields.append(item)
				line = '\t'.join(fields) + '\n'
				file.write(line)
				file.flush()
		# ed while
		file.close()


	def _db_to_file(self,sql,filepath,append, use_filter):
		file = None
		if append == False:
			file = open(filepath,'w')
		else:
			file = open(filepath,'a')
		self.execute(sql)
		while(True):
			rows = self.fetchmany(1000)
			if len(rows) <= 0:
				break
			for row in rows:
				fields = []
				for item in row:
					#数据清洗
					if use_filter:
						fields.append(self.data_filter(item))
					#不清洗
					else:
						fields.append(self.to_str(item))
				line = '\t'.join(fields) + '\n'	
				file.write(line)
				file.flush()
		#END WHILE
		file.close()


	def check_query(self,sql,params):
		'''
			[duplicate function]
			检查SQL是否符合规范, 不要直接调用
		'''
		if len(params) != 0:
			try:
				for key in params:
					if params.get(key) == None:
						raise AttributeError, 'AttributeError:key(%s) in params has no Value' % (key)
					if sql.find(('%s' % key)) == -1:
						raise AttributeError, 'AttributeError:key(%s) in params is not in [SQL]:%s' % (key,sql)
					var = '%s' % key
					val = "%s" % params.get(key)
					sql = sql.replace(var,val)
			except AttributeError, ex:
				sys.stderr.write('%s\n' % ex)
				raise SystemError, ex
		return sql


	def Check_query(sql,params):
		'''
			check_query的静态版本
		'''
		if len(params) != 0:
			try:
				for key in params:
					if params.get(key) == None:
						raise AttributeError, 'AttributeError:key(%s) in params has no Value' % (key)
					if sql.find(('%s' % key)) == -1:
						raise AttributeError, 'AttributeError:key(%s) in params is not in [SQL]:%s' % (key,sql)
					var = '%s' % key
					val = "%s" % params.get(key)
					sql = sql.replace(var,val)
			except AttributeError, ex:
				sys.stderr.write('%s\n' % ex)
				raise SystemError, ex
		return sql
	Check_query = staticmethod(Check_query)


	def load_from_txt(self,filepath,tblname,replace=False,fields_trnr='\t'):
		'''
			load_to_txt的配对函数，用于把文件中的数据load进新数据源
			要求目标数据的schema已经存在
			目前只支持 all ---> mysql 与 all ---> hive方式
			tblname: 目标表名或是库.表 名
			replace: load数据时，遇见相同的行是否用新行替换，True表示替换
			fields_trnr: load时，数据文件字段分割符
		'''
		if self.conn_str[0] in ['mysql']:
			return self._load_to_mysql(filepath,tblname,replace,fields_trnr)
		if self.conn_str[0] in ['mssql']:
			sys.stderr.write('SQL Server Load Program is not ready yet\n')
		if self.conn_str[0] in ['hive']:
			return self._load_to_hive(filepath,tblname)


	def _load_to_mysql(self,filepath,tblname,replace,fields_trnr):
			predo = 'SET max_error_count = 0; '
			if replace == True:
				sql = "LOAD DATA LOCAL INFILE '%s' REPLACE INTO TABLE %s CHARACTER SET UTF8 FIELDS TERMINATED BY " \
					   "'%s' ENCLOSED BY '%s'; " % (filepath, tblname, fields_trnr, '\"')
			else:
				sql = "LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE %s CHARACTER SET UTF8 FIELDS TERMINATED BY " \
					   "'%s' ENCLOSED BY '%s'; " % (filepath, tblname, fields_trnr, '\"')  
			try:
				self.execute(predo)
			except Error, ex:
				raise SystemError, '(MySQL LOAD DATA ERROR)%s' % ex

			try:
				self.execute(sql)
				self.commit()
			except Error, ex:
				raise SystemError, '(MySQL LOAD DATA ERROR)%s' % ex


	def _load_to_hive(self,filepath,tblname):
			sql = 'LOAD DATA LOCAL INPATH ' + "'" + filepath + "'" + ' INTO TABLE ' + tblname
			try:
				self.execute(sql)
			except Thrift.TException, ex:
				raise SystemError, '(HIVE LOAD DATA ERROR)%s' % ex
   

	def _load_to_oracle(self,filepath,tblname):
		pass


	def Insert_as_select(src,dest,sql,tblname,replace = False, use_filter = True):
		'''
			此函数先对数据源src（hive, mysql, oracle, sqlserver）上执行一个查询
			然后在dest（仅限mysql）上根据查询的结果集迭代的执行insert
			这一对儿动作是一个原子操作
			目前支持 all ---> mysql 方式
			src:   源连接 DataObject对象
			dest:  目标连接 DataObject对象
			replace, use_filter同load_from_txt()
		'''
		if dest.conn_str[0] in ['mysql']:
			cursor = dest.conn.cursor()
			cursor.execute('set autocommit = 1')
			cursor.execute('set max_error_count = 0')
			cursor.close()
		
		src.execute(sql)
		while (1):
			#按源数据库的类型拿到要插入的数据集
			rows = src.fetchmany(1000)
			if len(rows) <= 0:
				break
			dataset = []
			for row in rows:
				inner_lst = []
				for item in row:
					#数据清洗
					if use_filter:
						item = src.data_filter(item)
					#不清洗
					else:
						item = src.to_str(item)
					inner_lst.append(item)
				dataset.append(inner_lst)
			#数据合并，按目标数据库类型的不同执行不同的合并函数
			try:
				if dest.conn_str[0] in ['mysql']:
					DataObject._Insert_to_mysql_select(dest,dataset,tblname,replace)
				if dest.conn_str[0] in ['hive']:
					raise ValueError, "ValueError:Hive does not support 'Insert' Mutipulation"
				if dest.conn_str[0] in ['Oracle']:
					raise SystemError, "Exit Request:Oracle does not yet supported"
			except (ValueError,SystemError), ex:
				sys.stderr.write('%s\n' % (ex))
				raise SystemError, ex
		return dest.rowcount
	Insert_as_select = staticmethod(Insert_as_select)


	def _Insert_to_mysql_select(dest,dataset,tblname,replace):
		if replace == True:
			for row in dataset:
				cursor = dest.conn.cursor()
				sql = "REPLACE INTO %s VALUES (" % (tblname)
				for item in row:
					if item in ["''",'""']:
						sql += "'',"
					else:
						sql += "'%s'," % (item)
				sql = sql[:-1] + ');'
				cursor.execute(sql)
				dest.rowcount += 1
				cursor.close()
		else:
			for row in dataset:
				cursor = dest.conn.cursor()
				sql = "INSERT IGNORE INTO %s VALUES (" % (tblname)
				for item in row:
					if item in ["''",'""']:
						sql += "'',"
					else:
						sql += "'%s'," % (item)
				sql = sql[:-1] + ');'
				cursor.execute(sql)
				dest.rowcount += 1
				cursor.close()
	_Insert_to_mysql_select = staticmethod(_Insert_to_mysql_select)


	def _Insert_to_oracle_select(dest,dataset,tblname):
		pass
	_Insert_to_oracle_select = staticmethod(_Insert_to_oracle_select)


	def fetchmany(self,count):
		'''
			没什么，就是fetchmany
			count为一次迭代取的行数
		'''
		if not self.cursor:
			raise NameError, 'self.cursor is not defined!\n'
		result = []
		if self.conn_str[0] == 'mssql':
			result = self.cursor.fetchmany(count)
			self.rowcount = self.cursor.rowcount

		if self.conn_str[0] == 'mysql':
			result = self.cursor.fetchmany(count)
			self.rowcount += len(result)

		if self.conn_str[0] == 'oracle':
			result = self.cursor.fetchmany(count)
			self.rowcount = self.cursor.rowcount

		if self.conn_str[0] == 'hive':
			try:
				rows = self.cursor.fetchN(count)
				if len(rows) <= 0:
					return result
				for row in rows:
					list = row.split('\t')
					result.append(list)
				self.rowcount += len(rows)
			except Thrift.TException, tx:
				self.close()
				sys.stderr.write('%s\n' % (tx))
				raise SystemError, tx
		return result


	def fetchone(self):
		'''
			没什么，就是fetchone
		'''
		if not self.cursor:
			raise NameError, 'self.cursor is not defined!'
		result = []
		if self.conn_str[0] == 'mssql':
			result = self.cursor.fetchone()
			self.rowcount = self.cursor.rowcount

		if self.conn_str[0] == 'mysql':
			result = self.cursor.fetchone()
			self.rowcount += 1

		if self.conn_str[0] == 'oracle':
			result = self.cursor.fetchone()
			self.rowcount = self.cursor.rowcount

		if self.conn_str[0] == 'hive':
			try:
				row = self.cursor.fetchOne()
				if len(row) <= 0:
					return result
				result = row.split('\t')
				self.rowcount += 1
			except Thrift.TException, tx:
				self.close()
				sys.stderr.write('%s\n' % (tx))
				raise SystemError, tx
		return result


	def fetchall(self):
		'''
			没什么，就是fetchall
		'''
		if not self.cursor:
			raise NameError, 'self.cursor is not defined!'
		result = []
		if self.conn_str[0] == 'mssql':
			result = self.cursor.fetchall()
			self.rowcount = self.cursor.rowcount
		
		if self.conn_str[0] == 'mysql':
			result = self.cursor.fetchall()
			self.rowcount = len(result)

		if self.conn_str[0] == 'oracle':
			result = self.cursor.fetchall()
			self.rowcount = self.cursor.rowcount

		if self.conn_str[0] == 'hive':
			try:
				rows = self.cursor.fetchAll()
				if len(rows) <= 0:
					return result
				for row in rows:
					list = row.split('\t')
					result.append(list)
				self.rowcount = len(rows)
			except Thrift.TException, tx:
				self.close()
				sys.stderr.write('%s\n' % (tx))
				raise SystemError, tx
		return result


	def rows_affected(self):
		'''
			返回影响行数（整数）
		'''
		return self.rowcount


	def to_str(self,field):
		if field in ['',"''",None]:
			field = '""'
		if self.conn_str[0] in ['mysql','mssql']:
			field = unicode(field)
		if self.conn_str[0] == 'oracle':
			field = str(field)
		if self.conn_str[0] == 'hive':
			pass
		return field


	def data_filter(self,field):
		'''
			数据清洗，不要直接调用
		'''
		if field in [None,'','"',"'","''"]:
			field = '""'
		else:
			#从db拿到的数据有各种类型，要统一转成字符串
			if self.conn_str[0] in ['mysql','mssql']:
				field = unicode(field)
			if self.conn_str[0] in ['oracle']:
				field = str(field)
			#从hive拿到的数据本身是字符串，不用转
			if self.conn_str[0] == 'hive':
				pass
			#不将双引号本身转义
			if len(field) >= 3:
				field = field.replace('"','')
				field = field.replace("'",'')

			#控制字符过滤
			#ctrl + A/a --- ctrl + Z/z
			field = re.sub(r'\x01|\x02|\x03|\x08|\x09|\x0A|\x0B|\x0C|\x0D|\x1B|\x1C|\x1D|\x1E|\x1F|\\', 
							'', field)
		return field


	def close(self):
		'''
			关闭DataObject连接
		'''
		if self.conn:
			self.close_cursor()
			self.conn.close()
		self.rowcount = 0
		self.conn = None
 

	def disconnect(self):
		'''
			close的向下兼容版本
		'''
		self.close()


	def __del__(self):
		'''
			析构函数, 回收连接相关资源
		'''
		self.close()



def hive_exmp():
	'''
		我们的21集群没权限系统，所以无用户名和密码形参
	'''
	conn = DataObject.Connect(dbtype='hive', host='localhost', port=10010)
	conn.execute('use addr')
	conn.execute('select * from user_addr_in_order limit 2')
	for row in conn.fetchall():
		print '\t'.join(map(str, row))
	conn.close()


def mysql_exmp():
	'''
		dbtype, host, user, password, port是必填项
	'''
	conn = DataObject.Connect(dbtype='mysql', host='10.255.253.16', user='readuser', password='ru@r&d', \
							  port=3306, database='AntiFraud', charset='utf8')
	conn.execute('show tables')
	for row in conn.fetchall():
		print '\t'.join(map(str, row))
	conn.close()


def oracle_exmp():
	'''
		要求使用dsn去连接oracle
		dbtype, dsn, user, password是必填项
	'''
	conn = DataObject.Connect(dbtype='oracle', dsn='reportstaging.idc2:1521/staging1', user='v_stage', \
							  password='v_stage') 
	conn.execute('select table_name from user_tables')
	for row in conn.fetchall():
		print '\t'.join(map(str, row))
	conn.close()


def sqlserver_exmp():
	'''
		dbtype, host, user, password, port是必填项
	'''
	conn = DataObject.Connect(dbtype='mssql', host='172.16.128.86', user='readuser', password='password', port=1433, \
							  database='customer', charset='utf8')
	conn.execute('select top(2) * from customers')
	for row in conn.fetchall():
		print '\t'.join(map(str, row))
	conn.close()


def main():
	print '由于连接或端口限制，以下所有例子，只保证在21上执行成功'
	print '************ hive example **************'
	hive_exmp()
	print '************ mysql example *************'
	mysql_exmp()
	print '************ oracle example ************'
	oracle_exmp()
	print '************ sqlserver example *********'
	sqlserver_exmp()
	print '***************** end ******************'


if __name__ == '__main__':
	main()
