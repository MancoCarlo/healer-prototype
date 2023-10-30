import os 
import datetime

from subprocess import PIPE, Popen

dataset = "/ingestion/"
ct = datetime.datetime.now()
total = datetime.datetime.min
for dir in os.listdir(dataset):
	path = os.path.join(os.sep, 'user', dir + '/')
	put = Popen(["hadoop", "fs", "-mkdir", path], stdin=PIPE, bufsize=-1)
	put.communicate()
	print("Transfer ", dir)
	for root, dirs, files in os.walk(dataset + dir):
		for directory in dirs:
			sub = os.path.join(path, directory)
			put = Popen(["hadoop", "fs", "-mkdir", sub], stdin=PIPE, bufsize=-1)
			put.communicate()
		for file in files:
			path_file = os.path.join(root, file)
			ct1 = datetime.datetime.now()
			path_file = os.path.join(root,file)
			pathHD = dir + '/' + file[0:7] + '/'
			hdfs_path = os.path.join(os.sep, 'user', pathHD)
			put = Popen(["hadoop", "fs", "-put", path_file, hdfs_path], stdin=PIPE, bufsize=-1)
			put.communicate()
			ct2 = datetime.datetime.now()
			delta = ct2 - ct1
			total = total + delta
print("Total analytical only transfer time: ", total)
print("Total transfer time: ", datetime.datetime.now() - ct)
