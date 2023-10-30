import spacy
import pandas as pd
import datetime
import pytextrank
from pywebhdfs.webhdfs import PyWebHdfsClient
from hdfs import InsecureClient

current = datetime.datetime.now()

client_hdfs = InsecureClient('http://namenode:9870')
path = 'mimic-iii-clinical/NOTEEVENTS.csv'

with client_hdfs.read(path, encoding = 'utf-8') as reader:
	df = pd.read_table(reader, sep = ',')
	nlp = spacy.load("en_core_web_sm")
	# add PyTextRank to the spaCy pipeline
	nlp.add_pipe("textrank")
	for i in range(len(df)):
		text = str(df.iloc[i].TEXT)
		doc = nlp(text)
		# examine the top-ranked phrases in the document
		keywords = ''
		for phrase in doc._.phrases[:10]:
    			keywords = keywords + ',' + str(phrase.text)
		df.iloc[i, df.columns.get_loc('TEXT')] = keywords

with client_hdfs.write('pNOTEEVENTS.csv', encoding = 'utf-8') as writer:
	df.to_csv(writer, sep = ';', index = None, header = True)

print('Total process time: ', datetime.datetime.now() - current)
