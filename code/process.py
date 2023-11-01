import os
import pandas as pd
import shutil
import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient
from hdfs import InsecureClient

current = datetime.datetime.now()

client_hdfs = InsecureClient('http://namenode:9870')
path = 'mimic-iii-waveform'
hdfs = PyWebHdfsClient(host='namenode',port='9870')
data = hdfs.list_dir('user/root/' + path)

file_statuses = data["FileStatuses"]

for item in file_statuses["FileStatus"]:
	subdata = hdfs.list_dir('user/root/' + path + '/' + str(item['pathSuffix']))
	file_statuses2 = subdata["FileStatuses"]
	for item2 in file_statuses2["FileStatus"]:
		file = str(item2['pathSuffix'])
		path_file = path + '/' + str(item['pathSuffix']) + '/' + file
		if file.endswith('hea.txt'):
			with client_hdfs.read(path_file, encoding = 'utf-8') as reader:
				df = pd.read_csv(reader, sep = ';')
				#Per i file segnale bisogna unire la riga delle unita misura alla riga di intestazione
				measure_units = df.iloc[[0]].to_records(index = False) #unit di misura
				df = df.replace('"', '')
				df = df.drop(labels = 0, axis = 0) #elimino riga delle unità di misura dal file
				#aggiorno l'header del file csv aggiungengo le unità di misura
				for i in range(len(df.columns)):
					measure_units[0][i] = measure_units[0][i].replace("(", "[")
					measure_units[0][i] = measure_units[0][i].replace(")", "]")
					df.rename(columns={ df.columns[i]: df.columns[i] + measure_units[0][i]}, inplace = True)
				patientID = file[1:7]
				time = file[19:24] + '-00'
				date = file[8:18]
				#combine date and time to create a TimeStamp
				timeH = datetime.datetime.strptime(time, '%H-%M-%S').time()
				dateH = datetime.datetime.strptime(date, '%Y-%m-%d')
				timestamp = dateH.combine(dateH, timeH)
				#Convertion Elapsed Time in H:m:s format
				df[df.columns[0]] = pd.to_numeric(df[df.columns[0]])
				df[df.columns[0]] = df[df.columns[0]].apply(lambda x: datetime.timedelta(seconds = x))
				#Elaborate TimeStamp for each sample of the signal
				df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x + timestamp)
				df.rename(columns = {df.columns[0]:'TimeStamp'}, inplace = True)
				#insert patientID in Signal file
				df.insert(0, "SUBJECT_ID", patientID, True)
				with client_hdfs.write('processed/' + file[0:7] + "/" + file[:-8] + '.csv', encoding = 'utf-8') as writer:
					df.to_csv(writer, sep = ';', index = None, header = True)

print('Total process time: ', datetime.datetime.now() - current)
