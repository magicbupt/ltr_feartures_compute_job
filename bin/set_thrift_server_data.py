
import sys

class SetThriftData():
	def __init__(self):
		self.job_name = "thrift data"

	def saveData(self, srcfilepath, dispath):
		srcfile = None
		disfile_array = None
		try:
			srcfile = open(srcfilepath, 'r')
			disfile_array = [open('%s%s' % (dispath, i), 'w+') for i in range(10000)]
			while True:
				line = srcfile.readline()
				if not line:
					break
				array = line.strip("\n").split("\t")
				pid = int(array[0])
				index = pid%10000
				disfile_array[index].write(line)
		except Exception,ex:
			print ex
		finally:
			if srcfile:
				srcfile.close()
			if disfile_array:
				for i in range(len(disfile_array)):
					if disfile_array[i]:
						disfile_array[i].close()


def main():
	if len(sys.argv) != 2:
		return
	curday = sys.argv[1]	
	srcfilepath = "../data/product_info_%s" % curday
	dispath = "../data/thrift_data/temp_data_"
	o = SetThriftData()
	o.saveData(srcfilepath, dispath)

if __name__=="__main__":
	main()			
