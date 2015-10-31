'''
Created on Oct 30, 2015

To execute: bin/spark-submit --master local[4] MyFirstSparkJob.py

@author: dyerke
'''
from pyspark.context import SparkContext

if __name__ == '__main__':
    logFile = "/usr/local/spark/README.md"  # Should be some file on your system
    #
    sc= SparkContext("local", "Simple App")
    logData= sc.textFile(logFile).cache()
    #
    countAs= logData.filter(lambda x: 'a' in x).count()
    countBs= logData.filter(lambda x: 'b' in x).count()
    #
    print("Lines with a: %i, lines with b: %i" % (countAs, countBs))
