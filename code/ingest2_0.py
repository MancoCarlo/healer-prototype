import os
import datetime

from subprocess import PIPE, Popen

dataset = "/ingestion/"
ct = datetime.datetime.now()
total = datetime.datetime.min
for dir in os.listdir(dataset):
	print("Transfer ", dir)
	put = Popen(["hadoop", "fs", "-put", dataset + dir, dir], stdin=PIPE, bufsize=-1)
	put.communicate()
print("Total analytical only transfer time: ", total)
print("Total transfer time: ", datetime.datetime.now() - ct)
