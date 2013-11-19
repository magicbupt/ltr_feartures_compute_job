#include<iostream>
#include<string>
#include<map>
#include<vector>
#include <fstream>

#include"nokey_static_hash.h"

using namespace std;


inline void split(const string& str, const string& sp, vector<string>& out) {
	out.clear();
	string s = str;
	size_t beg, end;
	while (!s.empty()) {
		beg = s.find_first_not_of(sp);
		if (beg == string::npos) {
			break;
		}
		end = s.find(sp, beg);
		out.push_back(s.substr(beg, end - beg));
		if (end == string::npos) {
			break;
		}
		s = s.substr(end, s.size() - end);
	}
}

int main(int argc, char** argv)
{
	string srcfilepath = "../data/pid_ctr"; //srcfile path
	ifstream fin(srcfilepath.c_str());
	if(!fin)
	{
		//LOG_MSG_INFO("\topen srcfile:pid_ranking_val.txt fail");
		cout<<"open srcfile:search_data_log fail"<<endl;
		return -1;
	}

	//tmp map for read data from srcfile to memory
	map<int, double> m_pid_rankvalue;
	string line;
	while(getline(fin, line)){
		vector<string> tmp_vec;
		split(line, "\t", tmp_vec);
		if(tmp_vec.size() != 2)
		{
			continue;
		}

		int pid = atoi(tmp_vec[0].c_str());
		double rank_val = atof(tmp_vec[1].c_str());

		m_pid_rankvalue[pid] = rank_val;
	}

	static_hash_map<int, double> shm_pid_rankvalue; //static_hash_map 
	if(shm_pid_rankvalue.container_to_hash_file(m_pid_rankvalue, 15, "../data/pid2rankvalue"))
	{
		//LOG_MSG_INFO("\twrite hash file success");
		cout<<"write hash file success"<<endl;
	}
	else
	{
		//LOG_MSG_ERROR("\twrite hash file fail!");
		cout<<"write hash file fail"<<endl;
	}

	/*	
	cout<<"i am here"<<endl;
	static_hash_map<int, double> shm_pid2value;
 	shm_pid2value.load_serialized_hash_file("../data/pid2rankvalue", 0);
	cout<<shm_pid2value.size()<<endl;
	cout<<shm_pid2value[479]<<endl;
	//LOG_MSG_INFO("WRITE HASH VALUE TO FILE END");
	*/
	cout<<"WRITE HASH VALUE TO FILE END"<<endl;

	return 0;
}
