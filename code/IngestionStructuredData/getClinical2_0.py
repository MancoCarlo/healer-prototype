import os 
import datetime

from subprocess import PIPE, Popen

data = 'mimic-iii-clinical/*'
dataset = os.path.join(os.sep, 'user', data)

ct = datetime.datetime.now()
total = datetime.datetime.min

put = Popen(["hadoop", "fs", "-copyToLocal", dataset, '/ingestion/mimic-iii-clinical_p/'], stdin=PIPE, bufsize=-1)
put.communicate()
print("Total analytical only transfer time: ", total)
print("Total transfer time: ", datetime.datetime.now() - ct)
