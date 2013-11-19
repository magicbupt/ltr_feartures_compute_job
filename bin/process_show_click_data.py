

class CalShowClick():
	def __init__(self):
		self.query_click = {}
		self.query_show = {}
		self.query_position = {}
		self.query_searchtime = {}

	def getClickData(self, srcpath):
		file = None
		try:
			file = open(srcpath, 'r')
			while True:
				line = file.readline()
				if not line:
					break
				
				array = line.strip().split('\t')
				query = array[0].replace(" ","").replace("\t", "").replace("\n", "");
				dict_pid_click = {}
				for i in range(1, len(array)):
					pid_click_str = array[i].strip().split(':')
					if len(pid_click_str) != 2:
						continue

					pid = str2int(pid_click_str[0])
					n_click = str2int(pid_click_str[1])
					dict_pid_click[pid] = n_click
					
				if query not in self.query_click:
					self.query_click[query] = dict_pid_click
				#print query
		except Exception,ex:
			print ex
		finally:
			if file:
				file.close()

	def getShowData(self, srcpath):
		file = None
		try:
			file = open(srcpath, 'r')
			while True:
				line = file.readline()
				if not line:
					break

				array = line.strip().split('|')
				lenght = len(array)
				if lenght < 2:
					continue

				query = array[0].replace(" ","").replace("\t", "");
				n_search = array[1]
				dict_pid_show = {}
				dict_pid_pos = {}
				for i in range(2, lenght):
					tmp_str = array[i].strip().split(':')
					if len(tmp_str) != 3:
						break

					pid = str2int(tmp_str[0])
					pos = str2int(tmp_str[1])
					n_show = str2int(tmp_str[2])
					dict_pid_show[pid] = n_show
					dict_pid_pos[pid] = pos

				self.query_show[query] = dict_pid_show
				self.query_searchtime[query] = n_search
				self.query_position[query] = dict_pid_pos
				
				#print query
		except Exception,ex:
			print ex
		finally:
			if file:
				file.close()
	def saveData(self, dispath):
		file = None
		try:
			file = open(dispath, "w+")
			for query, pid_show_dict in self.query_show.items():

				n_search = 0
				if query in self.query_searchtime:
					n_search = self.query_searchtime[query]
				
				for pid, n_show in pid_show_dict.items():
					pos = self.query_position[query][pid]
					n_click = 0
					if query in self.query_click:
						if pid in self.query_click[query]:
							n_click = self.query_click[query][pid]

					file.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (query,\
						 n_search, pid, pos, n_show, n_click))

		except Exception,ex:
			print ex
		finally:
			if file:
				file.close()
				

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
	print "go ..."
	o = CalShowClick()
	show_filepath = "../data/show_click_tmp_data/log-20131031_search.clean"
	click_filepath = "../data/show_click_tmp_data/search-131031"
	dispath = "../data/query_data"
	print "show time ..."
	o.getShowData(show_filepath)
	print "click time ..."
	o.getClickData(click_filepath)	
	print "save data .."
	o.saveData(dispath)
	print "ok !"



if __name__ == "__main__":
	main()
