from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

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

# for obj in bucket.objects.filter(Prefix='dse_traffic/station_5min/2010'):
#     print('{0}:{1}'.format(bucket.name, obj.key))

config_dict = {"fs.s3n.awsAccessKeyId":aws_id,
               "fs.s3n.awsSecretAccessKey":aws_secret_key}

# Example of loading S3A file
lines = sc.textFile("s3n://%s:%s@%s/%s" % (aws_id, aws_secret_key, bucket_name, "dse_traffic/station_5min/2010/d8/d08_text_station_5min_2010_06_21.txt.gz"))
lines_nonempty = lines.filter(lambda x: len(x) > 0)
lines_nonempty.count()
