import datetime

__author__ = 'kai'

from sqlalchemy import create_engine
import numpy as np
import pandas as pd



factdb = create_engine("mysql+mysqldb://factread:r3adfac!@129.194.168.95/factdata")

print("Connected to runDB")
print(factdb.table_names())

#read source table
sourceDB = pd.read_sql_table('Source', factdb, columns=["fSourceName", "fSourceKEY"])

#print(sourceDB)

#valid time range

firstNight = "20130630"
lastNight = "20140204"
name = "Crab"
filename = "drs_mapping.json"


columns = ["fNight", "fROI", "fRunTypeKey", "fRunStart", "fRunStop", "fSourceKEY",
         "fCurrentsDevMean", "fMoonZenithDistance", "fThresholdMedian", "fEffectiveOn", "fCurrentsMedMean",
         "fZenithDistanceMean", "fDrsStep", "fRunID", "fOnTime", "fHasDrsFile", "fTriggerRateMedian", "fThresholdMinSet"]

print("Reading Data from DataBase from " + str(firstNight) + " to " + str(lastNight) + " for source: " + name)
rundb = pd.read_sql("SELECT " + ",".join(columns) + " from RunInfo WHERE (fNight > " + firstNight + " AND fNight <" + lastNight + ") ", factdb)


#lets try to get all data runs from a specific source between two dates

source = sourceDB[sourceDB.fSourceName == name]


conditions = [
    "fRunTypeKey == 1", # Data Events
    "fMoonZenithDistance > 100",
    #"fCurrentsMedMean < 7", # low light conditions hopefully
    "fROI == 300",
    "fZenithDistanceMean < 30",
    "fSourceKEY == " + str(int(source.fSourceKEY)),
    "fTriggerRateMedian > 40",
    "fTriggerRateMedian < 85",
    "fOnTime > 0.95",
    "fThresholdMinSet < 350"
]
querystring = " & ".join(conditions)
#print(querystring)

#get all data runs
data = rundb.query(querystring)


# now lets get all drs runs
conditions = [
    "fRunTypeKey == 2", #  300 roi Drs files
    "fROI == 300",
    "fDrsStep == 2",
    "fZenithDistanceMean < 30",
    "fSourceKEY == " + str(int(source.fSourceKEY)),
]
querystring = " & ".join(conditions)
drs_data = rundb.query(querystring)

print("Got " + str(len(data)) + " data entries and " + str(len(drs_data)) + " drs entries")
#reindex and fill to either previous or later observation

data = data.set_index("fRunStart")
drs_data = drs_data.set_index("fRunStart")

#write filenames
data["filename"] = data.fNight.astype(str) + "_" + data.fRunID.astype(str)
drs_data["filename"] = drs_data.fNight.astype(str) + "_" + drs_data.fRunID.astype(str)

#sorting
print("Sorting Data from Database")
data = data.sort()
drs_data = drs_data.sort()

print("Reindexing")
#get earlier and later drs observations
earlier_drs_entries = drs_data.reindex(data.index, method="ffill")
later_drs_entries = drs_data.reindex(data.index, method="backfill")

#now get the closest observations
earlier_drs_entries["deltaT"] = np.abs(earlier_drs_entries.fRunStop - data.fRunStop)
later_drs_entries["deltaT"] = np.abs(later_drs_entries.fRunStop - data.fRunStop).fillna(earlier_drs_entries.deltaT)

d_later = later_drs_entries[later_drs_entries.deltaT < earlier_drs_entries.deltaT]
d_early = earlier_drs_entries[later_drs_entries.deltaT >= earlier_drs_entries.deltaT]

closest_drs_entries = pd.concat([d_early, d_later])


#write filenames as json file
print("Writing data to " + str(filename))
mapping = pd.concat([closest_drs_entries.filename, earlier_drs_entries.filename, later_drs_entries.filename],
                    axis=1, keys=["closest", "earlier", "later"])
mapping = mapping.set_index(data.filename)
mapping.to_json(filename)

effective_on_time = (data.fOnTime * data.fEffectiveOn).sum()
print("Total effective onTime: " + str(datetime.timedelta(seconds=effective_on_time)))
