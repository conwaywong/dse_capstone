from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import time, datetime
from Stats2 import VecStat
import pickle
import numpy as np

sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)

fields = [StructField("ID", IntegerType(), True),
          StructField("year", IntegerType(), True),
          StructField("DOY", IntegerType(), True)]

for h in xrange(0, 24):
    for m in xrange(0, 60, 5):
        fields.append(StructField('samp_'+time(hour=h, minute=m).isoformat(), IntegerType(), True))
for h in xrange(0, 24):
    for m in xrange(0, 60, 5):
        fields.append(StructField('obso_'+time(hour=h, minute=m).isoformat(), IntegerType(), True))
for h in xrange(0, 24):
    for m in xrange(0, 60, 5):
        fields.append(StructField('flow_'+time(hour=h, minute=m).isoformat(), IntegerType(), True))
for h in xrange(0, 24):
    for m in xrange(0, 60, 5):
        fields.append(StructField('occ_'+time(hour=h, minute=m).isoformat(), FloatType(), True))
for h in xrange(0, 24):
    for m in xrange(0, 60, 5):
        fields.append(StructField('speed_'+time(hour=h, minute=m).isoformat(), FloatType(), True))

schema = StructType(fields)

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
        outK.append(int(data[1])) # Station ID
        outK.append(ts.year)
        outK.append(ts.timetuple().tm_yday)

        outV.append(time(hour=ts.hour, minute=ts.minute))
        outV.append(intOrZero(data[7])) # Samples
        outV.append(intOrZero(data[8])) # % Observed
        outV.append(intOrZero(data[9])) # Total Flow
        outV.append(floatOrZero(data[10])) # Avg Occupancy
        outV.append(floatOrZero(data[11])) # Avg Speed

        # TODO: Think about how to add other features

        yield tuple(outK), outV

def buildRow(tuples):
    key = tuples[0]
    vals = sorted(tuples[1], key=lambda time_val: time_val[0])

    res = [key[0], key[1], key[2]] + \
          [v[1] for v in vals] + \
          [v[2] for v in vals] + \
          [v[3] for v in vals] + \
          [v[4] for v in vals] + \
          [v[5] for v in vals]

    return res

def EigMap(inRow):
    s = VecStat(1440)
    s.accum(np.array(inRow[3:]))

    yield 0, s

def EigReduce(v1, v2):
    cStats = VecStat(1440)

    cStats.add(v1)
    cStats.add(v2)

    return cStats

def toCSVLine(data):
    return ','.join(str(d) for d in data)

lines = sc.textFile('../../jsg_test/data-in/station_5min/d11_text_station_5min_2010_01_*.txt.gz', 8)
newrows = lines.flatMap(parseInfo).groupByKey().map(buildRow)

newrows.map(toCSVLine).saveAsTextFile('d11_2010_01_m1_pivot')

stats = newrows.flatMap(EigMap).reduceByKey(EigReduce).collect()

x=20
eigs = stats[0][1].compute(k=x)

with open('d11_2010_01_m1_eigs'+str(x)+'.pkl', 'wb') as pfile:
    pickle.dump(eigs['eigvalues'], pfile)
    pickle.dump(eigs['eigvectors'], pfile)
    pickle.dump(eigs['mean'], pfile)
