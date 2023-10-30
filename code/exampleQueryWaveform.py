import pandas as pd
import datetime
import os
import traceback
from pywebhdfs.webhdfs import PyWebHdfsClient
from hdfs import InsecureClient

ct = datetime.datetime.now()

client_hdfs = InsecureClient('http://namenode:9870')

patientID = "000033"
waveformName = "p000033-2116-12-24-12-35n"
start = '2116-12-24 12:42:00'
end = '2116-12-24 15:00:00'
critical_value = 20

with client_hdfs.read('mimic-iii-clinical/ADMISSIONS.csv', encoding = 'utf-8') as reader:
	dfAdmission = pd.read_csv(reader)
	dfAdmission = dfAdmission[dfAdmission['SUBJECT_ID'] == int(patientID)]

with client_hdfs.read('mimic-iii-clinical/PATIENTS.csv', encoding = 'utf-8') as reader:
	dfPatient = pd.read_csv(reader)
	dfPatient = dfPatient[dfPatient['SUBJECT_ID'] == int(patientID)]

try: #if is present in processed
	with client_hdfs.read('processed/p' + patientID + '/' + waveformName + '.csv', encoding = 'utf-8') as reader:
		dfWaveform = pd.read_csv(reader, sep = ';')
except: #process from raw data
	#process and read
	print('Waveform: ' + waveformName +' not processed yet')
	try:
		with client_hdfs.read('mimic-iii-waveform/p' + patientID + '/' + waveformName + '.hea.txt', encoding = 'utf-8') as reader:
			df = pd.read_csv(reader, sep = ';')
			df = df.replace('"', ' ')
			measure_units = df.iloc[[0]].to_records(index = False) #unit di misura
			df = df.drop(labels = 0, axis = 0) #elimino riga delle unità di misura dal file
			#aggiorno l'header del file csv aggiungengo le unità di misura
			for i in range(len(df.columns)):
				measure_units[0][i] = measure_units[0][i].replace("(", "[")
				measure_units[0][i] = measure_units[0][i].replace(")", "]")
				df.rename(columns={ df.columns[i]: df.columns[i] + measure_units[0][i]}, inplace = True)
			time = waveformName[19:24] + '-00'
			date = waveformName[8:18]
			#combine date and time to create a TimeStamp
			timeH = datetime.datetime.strptime(time, '%H-%M-%S').time()
			dateH = datetime.datetime.strptime(date, '%Y-%m-%d')
			print('Processing from raw data')
			timestamp = dateH.combine(dateH, timeH)
			#Convertion Elapsed Time in H:m:s format
			df[df.columns[0]] = pd.to_numeric(df[df.columns[0]])
			df[df.columns[0]] = df[df.columns[0]].apply(lambda x: datetime.timedelta(seconds = x))
			#Elaborate TimeStamp for each sample of the signal
			df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x + timestamp)
			df.rename(columns = {df.columns[0]:'TimeStamp'}, inplace = True)
			#insert patientID in Signal file
			df.insert(0, "SUBJECT_ID", patientID, True)
			with client_hdfs.write('processed/p' + patientID + '/' + waveformName + '.csv', encoding = 'utf-8') as writer:
					df.to_csv(writer, sep = ';', index = None, header = True)
			with client_hdfs.read('processed/p' + patientID + '/' + waveformName + '.csv', encoding = 'utf-8') as reader:
                			dfWaveform = pd.read_csv(reader, sep = ';')
	except Exception:
		print('Waveform: ' + waveformName +' not found')
		pass
	pass

dfWaveform['SUBJECT_ID'] = pd.to_numeric(dfWaveform['SUBJECT_ID'])
dfWaveform['RESP[pm]'] = pd.to_numeric(dfWaveform['RESP[pm]'])
start_datetime = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
end_datetime = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')

dfWaveform['TimeStamp'] =  pd.to_datetime(dfWaveform['TimeStamp'], format='%Y-%m-%d %H:%M:%S')

dfWaveform = dfWaveform[dfWaveform['TimeStamp'].between(start_datetime, end_datetime)]
dfWaveform = dfWaveform[dfWaveform['RESP[pm]'] > critical_value]
result = dfWaveform.merge(dfAdmission, on='SUBJECT_ID')
print(result)

print("Total query time: ", datetime.datetime.now() - ct)
