# Databricks notebook source exported at Tue, 29 Dec 2015 22:55:45 UTC
from datetime import time, datetime
from pyspark.mllib.linalg import Vectors

# COMMAND ----------

def floatOrZero(f):
    try:
        return float(f)
    except ValueError:
        return 0.0

def intOrZero(i):
    try:
        return int(i)
    except ValueError:
        return 0

def parseInfo(line):
    outK = []
    outV = []

    data = line.split(',')

    if data[5] == 'ML': # Lane Type
        ts = datetime.strptime(data[0], '%m/%d/%Y %H:%M:%S') # 01/01/2010 00:00:00
        outK.append(floatOrZero(data[1])) # Station ID
        outK.append(floatOrZero(ts.year))
        outK.append(floatOrZero(ts.timetuple().tm_yday))

        outV.append(time(hour=ts.hour, minute=ts.minute))
        outV.append(floatOrZero(data[7])) # Samples
        outV.append(floatOrZero(data[8])) # % Observed
        outV.append(floatOrZero(data[9])) # Total Flow
        outV.append(floatOrZero(data[10])) # Avg Occupancy
        outV.append(floatOrZero(data[11])) # Avg Speed

        # TODO: Think about how to add other features

        yield tuple(outK), outV

def buildRow(tuples):
    key = tuples[0]
    vals = sorted(tuples[1], key=lambda time_val: time_val[0])

    m_source_list=[key[0], key[1], key[2]] + \
           [v[1] for v in vals] + \
           [v[2] for v in vals] + \
           [v[3] for v in vals] + \
           [v[4] for v in vals] + \
           [v[5] for v in vals]
    return Vectors.dense(m_source_list)

# COMMAND ----------

m_file_name= '/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010'
lines = sc.textFile(m_file_name, minPartitions=4)
newrows = lines.flatMap(parseInfo).groupByKey().map(buildRow)

# COMMAND ----------

t= newrows.first()
print type(t), t

# COMMAND ----------

from pyspark.mllib.feature import PCA as PCAmllib

model = PCAmllib(2).fit(newrows)
transformed = model.transform(newrows)

# COMMAND ----------

t= transformed.first()
print type(t), t