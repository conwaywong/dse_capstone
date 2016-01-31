package org.ucsd.dse.capstone.anomaly

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

class KMeansOutlier extends AnomalyDetector
{
    def fit(X: RDD[Vector])
    {
        
    }
    
    def DetectOutlier(X : RDD[(Vector,Long)], stdd:Double = 1.0) : RDD[Vector] =
    {
        X.map{ case(o,i)=>o }
    }
}