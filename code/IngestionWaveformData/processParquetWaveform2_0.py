import os
import pandas as pd
import shutil
import datetime
dataset = '/ingestion/mimic-iii-waveform_p'

current = datetime.datetime.now()
destination2 = '/ingestion/processed/'
os.mkdir(destination2)

for root, dirs, files in os.walk(dataset):  # replace the . with your starting directory
	for file in files:
		path_file = os.path.join(root,file)
		if file.endswith('.hea'):
			f = open(path_file, "r")
			stri = f.readline()
			stri = stri.replace(" ", ";")
			stri = stri.replace("\n", "")
			w = open(path_file, "w")
			w.write(stri)
			f.close()
			w.close()

for i in os.listdir(dataset):
    os.mkdir(destination2 + i)
for root, dirs, files in os.walk(dataset):
    # replace the . with your starting directory
	for file in files:
		path_file = os.path.join(root,file)
		for fileB in files:
			if file.endswith('hea.txt') and fileB.endswith('hea.txt') == 0 and fileB in file:
				path_fileB = os.path.join(root, fileB)
                 # try:
				dfS = pd.read_parquet(path_file)
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
                        #insert patientID in Signal file
				dfS.insert(0, "PatientID", patientID[1:7], True)
                        #Convertion Elapsed Time in H:m:s format
				dfS[dfS.columns[1]] = dfS[dfS.columns[1]].apply(lambda x: datetime.timedelta(seconds = float(x)))
                        #Elaborate TimeStamp for each sample of the signal
				dfS[dfS.columns[1]] = dfS[dfS.columns[1]].apply(lambda x: x + timestamp)
				dfS.rename(columns = {dfS.columns[1]:'TimeStamp'}, inplace = True)
                        #dfS.to_csv(destination2 + file, sep =';', index=None, header = True)
				dfS.astype(str)
				dfS.to_parquet(destination2 + file[0:7] + "/" + file[:-8], index=None)
                  # except:
                       # print("Impossible to parse ", file)
                       # pass
print('Total process time: ', datetime.datetime.now() - current)
