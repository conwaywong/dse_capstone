# Databricks notebook source exported at Tue, 29 Dec 2015 18:44:18 UTC
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import time, datetime

# COMMAND ----------

#sc = SparkContext("local", "Simple App")
sqlContext = SQLContext(sc)

# COMMAND ----------

# MAGIC %md
# MAGIC Setup the Mount point on the cluster to access S3 Bucket

# COMMAND ----------

# NOTE: Set the access to this notebook appropriately to protect the security of your keys.
# Or you can delete this cell after you run the mount command below once successfully.
ACCESS_KEY = getArgument("1. ACCESS_KEY", "")
SECRET_KEY = getArgument("2. SECRET_KEY", "")
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = getArgument("3. S3_BUCKET","dse-jgilliii")
MOUNT_NAME = getArgument("4. MNT_NAME","jgilll")

# COMMAND ----------

dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC Build up the Schema of fields for the data rows

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC Assist Functions

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

    return [key[0], key[1], key[2]] + \
           [v[1] for v in vals] + \
           [v[2] for v in vals] + \
           [v[3] for v in vals] + \
           [v[4] for v in vals] + \
           [v[5] for v in vals]

# COMMAND ----------

# MAGIC %md
# MAGIC Real work

# COMMAND ----------

lines = sc.textFile("/mnt/%s/dse_traffic/station_5min/2015/d11/d11_text_station_5min_2010_01_0[1234567]*" % MOUNT_NAME)
newrows = lines.flatMap(parseInfo).groupByKey().map(buildRow)

df = sqlContext.createDataFrame(newrows, schema)
#df.registerTempTable("avg_speed")


#results = sqlContext.sql("SELECT ID, count(*) as cnt FROM avg_speed GROUP BY ID").collect()

#for r in sorted(results, key=lambda x: x.ID):
#    print r