import pandas as pd
import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from hdfs import InsecureClient

current = datetime.datetime.now()

client_hdfs = InsecureClient('http://namenode:9870')
path = 'pNOTEEVENTS.csv'
targetKey = '*'
with client_hdfs.read(path, encoding = 'utf-8') as reader:
	df = pd.read_csv(reader, sep = ';')

	#get patientID and admission time of the report with key = 'keyword'
	m = []
	for i in range(len(df)):
    		vet = []
    		text = df.iloc[i].TEXT
    		if targetKey in text:
        		patient = df.iloc[i].SUBJECT_ID
        		admtime = df.iloc[i].HADM_ID
        		vet.append(patient)
        		vet.append(admtime)
        		m.append(vet)

path = 'mimic-iii-clinical/NOTEEVENTS.csv'
with client_hdfs.read(path, encoding = 'utf-8') as reader:
        df = pd.read_csv(reader, sep = ',')

for i in range(len(m)):
    print(df[(df['SUBJECT_ID'] == m[i][0]) & (df['HADM_ID'] == m[i][1])].TEXT)

print('Total process time: ', datetime.datetime.now() - current)
