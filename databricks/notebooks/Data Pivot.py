# Databricks notebook source exported at Wed, 30 Dec 2015 06:55:09 UTC
# MAGIC %md
# MAGIC # Data Pivot

# COMMAND ----------

# Determing if findspark module is available. 
# If so, it's assumed this notebook is run on a local Spark instance and *not* on Databricks
DATABRICKS_FLAG = True
try:
    __import__("findspark")
    import findspark
    findspark.init()
    DATABRICKS_FLAG = False
    print "findspark module found. Running on local Spark instance."
except ImportError:
    print "findspark module not available. Running on Databricks cluster."

# COMMAND ----------

# A SparkContext instance is created automagically by Databricks
# If not running in Databricks, create a SparkContext instance
if not DATABRICKS_FLAG:
    sc = SparkContext("local", "Simple App")

# COMMAND ----------

'''
IMPORTANT

To prevent AWS credentials from being saved in this notebook, it's assumed there is a file in the current directory named 'spark.config' that contains AWS credentials.
Below is an example of the contents:

[aws]                                                          
access_key_id: <YOUR ACCESS KEY ID>
secret_access_key: <YOUR SECRET KEY>
region_name: us-west-1
bucket_id: dse-jgilliii

'''

import boto3
import botocore
from boto3.session import Session

import ConfigParser
config = ConfigParser.ConfigParser()
config.read("spark.config")
aws_id = config.get("aws", "access_key_id")
aws_secret_key = config.get("aws", "secret_access_key")
aws_region = config.get("aws", "region_name")
bucket_name = config.get("aws", "bucket_id")

session = Session(aws_access_key_id=aws_id,
                  aws_secret_access_key=aws_secret_key,
                  region_name=aws_region)

# Let's use Amazon S3
s3 = session.resource('s3')

# Verify the bucket is available
# boto3 does not have a convenience method to verify whether a bucket exists.  Hence the boilerplate code.
bucket = s3.Bucket(bucket_name)
bucket_exists = True
try:
    s3.meta.client.head_bucket(Bucket=bucket_name)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

# Example of iterating over files and filtering based on directory name
# for obj in bucket.objects.filter(Prefix='dse_traffic/station_5min/2015/d10'):
#     lines = sc.textFile("s3n://%s:%s@%s/%s" % (aws_id, aws_secret_key, bucket_name, obj.key))
#     lines_nonempty = lines.filter(lambda x: len(x) > 0)
#     print lines_nonempty.count()

# Example of loading s3n file.  We have yet to get it working with s3 or s3a
# Note the following does not seem to work with prebuilt Spark 1.5.2, Hadoop 2.6 distribution.  Downgrade to Hadoop 2.4.
lines = sc.textFile("s3n://%s:%s@%s/%s" % (aws_id, aws_secret_key, bucket_name, "dse_traffic/station_5min/2010/d8/d08_text_station_5min_2010_06_21.txt.gz"))
lines_nonempty = lines.filter(lambda x: len(x) > 0)
lines_nonempty.count()

# COMMAND ----------

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import time, datetime

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

# TODO Change reading from S3 bucket instead of virtual mount
#lines = sc.textFile("/mnt/%s/dse_traffic/station_5min/2015/d11/d11_text_station_5min_2010_01_0[1234567]*" % MOUNT_NAME)
#newrows = lines.flatMap(parseInfo).groupByKey().map(buildRow)