package org.ucsd.dse.capstone.anomaly.deprecated

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

trait AnomalyDetector
{
    def fit(X: RDD[Vector])
    def DetectOutlier(X : RDD[(Array[Int], Vector)], Y:Double) : RDD[(Array[Int], Vector)]
    /* TODO: Think about other interfaces
    def score()
    def get_params()
    def set_params()
    */
}