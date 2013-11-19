#!/usr/bin/env python
#encoding:utf8

import sys
import time

from data_access_object import DataObject

class DBFactory(object):
    '''
     多次连接数据库公用方法
     CONN_NUM 连接次数
     SLEEP_TIME 每次中断时间
    '''
    CONN_NUM = 3
    SLEEP_TIME = 5 

    @staticmethod
    def Connect(dbtype, host = '', dsn = '', database = '', charset = '', user = '', password = '', port = 0):
        '''
        输入连接参数连接数据库
        '''
        num = 0
        db_conn = None

        while(True):
            try:
                num += 1
                db_conn = DataObject.Connect(dbtype, host, dsn, database, charset, user, password, port)
                return db_conn
            except Exception,ex:
                if num <= DBFactory.CONN_NUM:
                    time.sleep(DBFactory.SLEEP_TIME)
                    num += 1
                else:
                    raise Exception, '(DB connect Error)%s' % ex
                    

    @staticmethod
    def connect_conf(config,dbkey):
        '''
        输入连接conf,以及连接对应的才conf中key连接
        '''
        num = 0
        db_conn = None

        while(True):
            try:
                num += 1
                db_conn = DBFactory._get_connect(config,dbkey)
                return db_conn
            except Exception,ex:
                if num <= DBFactory.CONN_NUM:
                    time.sleep(DBFactory.SLEEP_TIME)
                    num += 1
                else:
                    raise Exception, '(DB connect Error)%s' % ex

    @staticmethod
    def _get_connect(config,dbkey):
        db_type = config.get(dbkey, 'db_type')
        db_conf = eval(config.get(dbkey, 'db_conf'))
        db_conn = DataObject.Connect(db_type,
                                     host = db_conf.get('host'),
                                     dsn = db_conf.get('dsn',''),
                                     database = db_conf.get('db'),
                                     charset = db_conf.get('charset',''),
                                     user = db_conf.get('user'),
                                     password = db_conf.get('passwd'),
                                     port = db_conf.get('port'))
        return db_conn

