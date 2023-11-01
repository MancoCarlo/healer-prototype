import os
import pandas as pd
import shutil
import datetime
dataset = '/ingestion/mimic-iii-waveform'
os.mkdir('/ingestion/mimic-iii-waveform_p')
destination = '/ingestion/mimic-iii-waveform_p/'
current = datetime.datetime.now()

for i in os.listdir(dataset):
    os.mkdir(destination + i)
for root, dirs, files in os.walk(dataset):  # replace the . with your starting directory
	for file in files:
		path_file = os.path.join(root,file)
		if file.endswith('hea.txt'):
			try:
				df = pd.read_table(path_file, sep = ";")
				df = df.replace('"', ' ')
				df = df.applymap(lambda x: x.strip())
				df.columns = [c.strip() for c in df.columns]
				df.to_csv(path_file, sep =';', index=None, header = True)
			except:
				pass
			#Per i file segnale bisogna unire la riga delle unita misura alla riga di intestazione
			df = pd.read_table(path_file, ";", header = None)
			measure_units = df.iloc[[1]].to_records(index = False) #unit di misura
			df = pd.read_csv(path_file, ";")
			df = df.drop(labels = 0, axis = 0) #elimino riga delle unità di misura dal file
			#aggiorno l'header del file csv aggiungengo le unità di misura
			df.rename(columns={df.columns[0]:"Time"}, inplace=True)
			for i in range(len(df.columns)):
				measure_units[0][i] = measure_units[0][i].replace("(", "[")
				measure_units[0][i] = measure_units[0][i].replace(")", "]")
				df.rename(columns={ df.columns[i]: df.columns[i] + measure_units[0][i]}, inplace = True)
			#try:
			df = df.replace('"', '')
			df = df.astype(str)
			df.to_parquet(destination + file[0:7] + "/" + file, index=None)
			#except:
			#	print("Error, not converted: ", file)
			#	pass
			#shutil.copyfile(path_file, destination + path_file[34:41] + "/" + file)
		else:
			shutil.copyfile(path_file, destination + file[0:7] + "/" + file)
print('Total process time: ', datetime.datetime.now() - current)
