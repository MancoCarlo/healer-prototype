import os
import pandas as pd
import shutil
import datetime
dataset = '/ingestion/mimic-iii-waveform'

current = datetime.datetime.now()

os.mkdir('/ingestion/segnicsv')
destination = '/ingestion/segnicsv/'

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
			#try:
			df = df.replace('"', ' ')
			#except:
			#	pass
			df = df.drop(labels = 0, axis = 0) #elimino riga delle unità di misura dal file
			#aggiorno l'header del file csv aggiungengo le unità di misura
			df.rename(columns={df.columns[0]:"Time"}, inplace=True)
			for i in range(len(df.columns)):
				measure_units[0][i] = measure_units[0][i].replace("(", "[")
				measure_units[0][i] = measure_units[0][i].replace(")", "]")
				df.rename(columns={ df.columns[i]: df.columns[i] + measure_units[0][i]}, inplace = True)
			df = df.replace('"', '')
			df.to_csv(destination + file[0:7] + "/" + file,sep =';', index=None, header = True)
			#shutil.copyfile(path_file, destination + path_file[34:41] + "/" + file)
		else:
			#Per i file header viene catturata solo la prima riga e convertita in csv
			f = open(path_file, "r")
			str = f.readline()
			str = str.replace(" ", ";")
			str = str.replace("\n", "")
			w = open(destination + file[0:7] + "/" + file, "w")
			w.write(str)
f.close()
w.close()

dataset2 = '/ingestion/segnicsv/'
destination2 = '/ingestion/processed/'
os.mkdir(destination2)
for i in os.listdir(dataset2):
    os.mkdir(destination2 + i)
for root, dirs, files in os.walk(dataset2):
    # replace the . with your starting directory
	for file in files:
		path_file = os.path.join(root,file)
		for fileB in files:
			if file.endswith('hea.txt') and fileB.endswith('hea.txt') == 0 and fileB in file:
				print("Parsing: ", file)
				path_fileB = os.path.join(root, fileB)
				dfS = pd.read_csv(path_file, ";")
				dfH = pd.read_csv(path_fileB, ";", header = None)
                    		#extract patientID, time and date from header file
				patientID = dfH[0].to_string(index = None)
				time = dfH[4].to_string(index = None)
				date = dfH[5].to_string(index = None)
                        	#combine date and time to create a TimeStamp
				time = time[1:9]
				timeH = datetime.datetime.strptime(time, '%H:%M:%S').time()
				date = date[1:]
				dateH = datetime.datetime.strptime(date, '%d/%m/%Y')
				timestamp = dateH.combine(dateH, timeH)
				#Convertion Elapsed Time in H:m:s format
				dfS[df.columns[0]] = dfS[df.columns[0]].apply(lambda x: datetime.timedelta(seconds = x))
				#Elaborate TimeStamp for each sample of the signal
				dfS[df.columns[0]] = dfS[df.columns[0]].apply(lambda x: x + timestamp)
				dfS.rename(columns = {df.columns[0]:'TimeStamp'}, inplace = True)
				#insert patientID in Signal file
				dfS.insert(0, "PatientID", patientID[1:7], True)
                        	#dfS.to_csv(destination2 + file, sep =';', index=None, header = True)
				try:
					dfS.to_parquet(destination2 + file[0:7] + "/" + file[:-8], index=None)
				except:
					print("Not converted: ", file)
					pass
print('Total process time: ', datetime.datetime.now() - current)
#shutil.rmtree(dataset2)
#shutil.rmtree(dataset)
