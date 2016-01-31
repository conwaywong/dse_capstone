package org.ucsd.dse.capstone.anomaly
import org.ucsd.dse.capstone.traffic._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

object CalcMahala {
    def main(args: Array[String])
    {
        val template: SparkTemplate = new DefaultSparkTemplate()
        template.execute { sc => do_execute(sc) }
    }
    
    def do_execute(sc: SparkContext)
    {
        val files: List[String] = List("station_5min/d11_text_station_5min_2010_01_[01]*.txt.gz")
        val m_string_rdd: RDD[String] = MLibUtils.new_rdd(sc, files, 4)
     
        val handler: PivotHandler = new StandardPivotHandler(sc, Fields.Speed)
        val m_vector_rdd: RDD[Vector] = MLibUtils.pivot(m_string_rdd, handler)
        
        val sig:AnomalyDetector = new PCAMahalanobis(sc)
        val outliers = sig.DetectOutlier(m_vector_rdd.zipWithIndex(), 3.0)
        outliers.collect().foreach(println)
    }
}