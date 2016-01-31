package org.ucsd.dse.capstone.anomaly

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

trait AnomalyDetector {
    def fit(X: RDD[Vector])
    def DetectOutlier(X : RDD[(Vector,Long)], stdd:Double = 1.0) : RDD[Vector]
    /* TODO: Think about other interfaces
    def score()
    def get_params()
    def set_params()
    */
}