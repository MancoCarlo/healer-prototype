import os
import pandas as pd
import shutil
import datetime
dataset = '/ingestion/mimic-iii-clinical_p'

current = datetime.datetime.now()

try:
	os.mkdir('/ingestion/mimic-iii-clinical_parquet')
except:
	pass
destination = '/ingestion/mimic-iii-clinical_parquet/'

for root, dirs, files in os.walk(dataset):
	for file in files:
		path_file = os.path.join(root,file)
		print("processing: ", file)
		df = pd.read_csv(path_file, sep = ',')
		try:
			df.to_parquet(destination + file[:-4], index=None)
		except:
			print("Impossible to convert: ", file)
			pass
print('Total process time: ', datetime.datetime.now() - current)
#shutil.rmtree(dataset)

