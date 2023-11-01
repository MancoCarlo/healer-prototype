
import pandas as pd
import os
from datetime import datetime
from subprocess import PIPE, Popen
from pyarrow import fs

ct = datetime.now()
fsa = fs.HadoopFileSystem.from_uri('hdfs://172.18.0.3:9000/user=user')

patientID = "000020"
waveformName = "p000020-2183-04-28-17-47n"

with fsa.open('/user/mimic-iii-clinical/PATIENTS.csv') as reader:
	dfPatient = pd.read_parquet(reader)
with fsa.open('/user/mimic-iii-clinical/ADMISSIONS.csv') as reader:
        dfAdmission = pd.read_parquet(reader)
with fsa.open('/user/processed/p' + patientID + '/p000020-2183-04-28-17-47n') as reader:
        dfWaveform = pd.read_parquet(reader)

dfWaveform['TimeStamp'] =  pd.to_datetime(dfWaveform['TimeStamp'], format='%Y-%m-%d %H:%M:%S')
dfPatient = dfPatient[dfPatient['SUBJECT_ID'] == int(patientID)]
dfAdmission = dfAdmission[dfAdmission['SUBJECT_ID'] == int(patientID)]

start_datetime = datetime.strptime('2183-04-28 21:15:00', '%Y-%m-%d %H:%M:%S')
end_datetime = datetime.strptime('2183-04-28 21:23:00', '%Y-%m-%d %H:%M:%S')
dfWaveform = dfWaveform[dfWaveform['TimeStamp'].between(start_datetime, end_datetime)]
dfWaveform = dfWaveform[dfWaveform['RESP(pm)'] > 20]
dfWaveform = dfWaveform.rename(columns = { 'PatientID' : 'SUBJECT_ID'})

result = dfWaveform.merge(dfAdmission, on='SUBJECT_ID')
print(result)

print("Total query time: ", datetime.datetime.now() - ct)
