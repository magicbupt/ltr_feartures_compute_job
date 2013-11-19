#!/usr/bin/env python

import time, datetime

from get_product_info import ProductInfo
from process_show_click_data import CalShowClick

def saveData(product_info_obj, show_click_obj, dispath):
	file = None
	try:
		file = open(dispath, "w+")
		for query, pid_show_dict in show_click_obj.query_show.items():

			n_search = 0
			if query in show_click_obj.query_searchtime:
				n_search = show_click_obj.query_searchtime[query]

			for pid, n_show in pid_show_dict.items():
				pos = show_click_obj.query_position[query][pid]
				n_click = 0
				if query in show_click_obj.query_click:
					if pid in show_click_obj.query_click[query]:
						n_click = show_click_obj.query_click[query][pid]
				
				product_info = "0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
				if pid in product_info_obj.pid_info:
					product_info = product_info_obj.pid_info[pid]

				file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (query,\
					 n_search, pid, pos, n_show, n_click, product_info))

	except Exception,ex:
		print ex
	finally:
		if file:
			file.close()	


def prepareSVMRankFile(filepath, disfilepath, lenght, feature_num, feature_index):
	srcfile = None
	disfile = None

	qid = 0
	query_old = None
	try:
		srcfile = open(filepath, 'r')
		disfile = open(disfilepath, 'a+')
		flag = 0
		while True:
			line = srcfile.readline()
			if not line:
				break
			array = line.strip('\n').split("\t")

			if len(array) != feature_num:
				continue

			query = array[0]
			show_time = int(array[4])
			click_time = int(array[5])
			
			if query != query_old:
				qid += 1
				query_old = query
				flag = 0
				if int(array[6]) == 0:
					flag = 1

			if flag == 1:
				continue

			#str = "%s qid:%s" % (ctr, qid)
			str = "%s %s qid:%s" % (click_time, show_time, qid)
			for i in range(lenght):
				index_id = feature_index[i]
				str = "%s %s:%s" % (str, i+1, array[index_id])

			disfile.write("%s\n" % str)
	except Exception,ex:
		print ex
	finally:
		if srcfile:
			srcfile.close()
		if disfile:
			disfile.close()

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
		return  preday.strftime("%Y%m%d")
	except Exception ,ex:
		print 'get preday error~~'
		raise ex
	return


def main():
	
	print "go ..."
	data_path = "../data"
	
	curday = datetime.datetime.now().strftime('%Y-%m-%d')
	curday = getSpecifiedDay(curday, 1)

	print "product info ..."	
	product_info_obj = ProductInfo()
	product_info_obj.run()
	
	print "caculate show ..."
	show_click_obj = CalShowClick()
	
	show_filename = "log-%s_search.clean" % curday
	show_filepath = "%s/temp_data/%s" % (data_path, show_filename)
	show_click_obj.getShowData(show_filepath)

	print "caculate click..."	
	click_filename = "search-%s" % curday[2:]
	click_filepath = "%s/temp_data/%s" % (data_path, click_filename)
	show_click_obj.getClickData(click_filepath)

	print "save search info ..."
	search_log_filename = "search_log_%s" % curday
	dispath = "%s/%s" % (data_path, search_log_filename)
	saveData(product_info_obj, show_click_obj, dispath)

#	print "conver data to svmrank format ..."	
#	svmrank_filename = "svmrank_data_%s" % curday
#	svmrank_filepath = "%s/%s" % (data_path, svmrank_filename)
#	lenght = 16
#	feature_num = 23 
#	feature_index = [7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]	
#	prepareSVMRankFile(dispath, svmrank_filepath, lenght, feature_num, feature_index)
	print "end !"	
	



if __name__ == "__main__":
	main()	
