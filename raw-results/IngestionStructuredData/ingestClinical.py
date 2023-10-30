import os 
import datetime

from subprocess import PIPE, Popen

dataset = "/ingestion/"
ct = datetime.datetime.now()
total = datetime.datetime.min
for root, dirs, files in os.walk(dataset):
        for file in files:
            ct1 = datetime.datetime.now()
            path_file = os.path.join(root,file)
            hdfs_path = os.path.join(os.sep, 'user', 'mimic-iii-clinical/')
            put = Popen(["hadoop", "fs", "-put", path_file, hdfs_path], stdin=PIPE, bufsize=-1)
            put.communicate()
            ct2 = datetime.datetime.now()
            delta = ct2 - ct1
            print(delta, " to transfer the file: ", file)
            total = total + delta
print("Total analytical transfer time: ", total)
print("Total transfer time: ", datetime.datetime.now() - ct)
