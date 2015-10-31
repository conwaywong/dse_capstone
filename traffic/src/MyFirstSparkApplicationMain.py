'''
Created on Oct 30, 2015

@author: dyerke
'''
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

if __name__ == '__main__':
    m_hostname= "dyerke-Inspiron-7537"
    #
    conf= SparkConf()
    conf.setAppName("MyTestApp")
    conf.setMaster("spark://" + m_hostname + ":7077")
    conf.setSparkHome("/usr/local/spark")
    conf.set("spark.driver.host", m_hostname)
    logFile = "/usr/local/spark/README.md"  # Should be some file on your system
    #
    sc= SparkContext(conf=conf)
    logData= sc.textFile(logFile).cache()
    #
    countAs= logData.filter(lambda x: 'a' in x).count()
    countBs= logData.filter(lambda x: 'b' in x).count()
    #
    print("Lines with a: %i, lines with b: %i" % (countAs, countBs))
    sc.stop()