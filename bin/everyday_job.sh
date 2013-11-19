#!/bin/sh
PYTHON=/usr/local/bin/python

work_path="/d2/caiting/search_ranking/ltr_feartures_compute_job"

data_url="http://192.168.197.151:9030/joblog/data/" 
click_url="http://192.168.197.151:9030/joblog/click/detail/"
uv_url="http://192.168.197.176:9030/joblog/data/"

search_lastday=$(date -d yesterday +%Y%m%d)
uv_lastday=$(date -d yesterday +%Y-%m-%d)

search_log=log\-$search_lastday\_search.clean.tar.bz2
click_log=search\-${search_lastday:2:8}
day_uv_log=uv_d_$uv_lastday
week_uv_log=uv_w_$uv_lastday
month_uv_log=uv_m_$uv_lastday

echo $search_lastday

# load search log data from url 
echo "load show, click and uv data from url ..."
if [ $? -eq 0 ];then
	cd $work_path/data/temp_data
	wget $data_url$search_log
	wget $click_url$click_log
	wget $uv_url$day_uv_log
	wget $uv_url$week_uv_log
	wget $uv_url$month_uv_log
	tar -jxvf $search_log && rm -f *.tar.*
else
	exit -1
fi


#get each feature from mysql
echo "load product info from db and init search_log data ..."
if [ $? -eq 0 ];then
	cd $work_path/bin
	$PYTHON main.py 
else
	exit -2
fi 

# caculate ctr for each pid
echo "caculate ctr ..."
if [ $? -eq 0 ];then
	cd $work_path/bin
	$PYTHON caculate_features_for_model.py $search_lastday
else
	exit -3
fi

# change data to static_hashmap and then send to online search_engine
echo "change pid_ctr file to statichashmap and scp to search server..."
if [ $? -eq 0 ];then
	./map2hashmap
	scp ../data/pid2rankvalue search@192.168.85.131:/home/search/caiting/search_test/modules/search_ranking/
else
	exit -4
fi
 

# clear the search_log data
echo "clean temp_data/* ..."
if [ $? -eq 0 ];then
	#cd $work_path/data && rm -f product_info_$uv_lastday 
	cd $work_path/data/temp_data/ && rm -f *
else
	exit -5
fi

#save data for thrift server
echo "thrift data saving ..."
if [ $? -eq 0 ];then
	cd $work_path/bin
	$PYTHON set_thrift_server_data.py $search_lastday
else
	exit -6
fi

echo "ok !"
