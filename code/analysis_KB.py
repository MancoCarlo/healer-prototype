import pandas as pd
import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from hdfs import InsecureClient
from keybert import KeyBERT
import GPUtil
GPUs = GPUtil.getGPUs()
print(GPUs)
current = datetime.datetime.now()

client_hdfs = InsecureClient('http://namenode:9870')
path = 'mimic-iii-clinical/NOTEEVENTS.csv'

with client_hdfs.read(path, encoding = 'utf-8') as reader:
	df = pd.read_table(reader, sep = ',')
	kw_model = KeyBERT()

	for i in range(len(df)):
		text = str(df.iloc[i].TEXT)
		keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), stop_words=None)
		# examine the top-ranked phrases in the document
		keys = ''
		for word in keywords:
    			keys = keys + ', ' + word[0]
		df.iloc[i, df.columns.get_loc('TEXT')] = keys
		print('ok')

with client_hdfs.write('pNOTEEVENTS_KB.csv', encoding = 'utf-8') as writer:
	df.to_csv(writer, sep = ';', index = None, header = True)

print('Total process time: ', datetime.datetime.now() - current)
