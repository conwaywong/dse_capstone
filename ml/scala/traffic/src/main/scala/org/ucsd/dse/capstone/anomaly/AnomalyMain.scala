package org.ucsd.dse.capstone.anomaly
import org.ucsd.dse.capstone.traffic._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

import breeze.linalg.{DenseVector => BDV}

object AnomalyMain
{
    private def parseVector(line: String): Vector = 
    {
        Vectors.dense(line.split(',').map(_.toDouble))
    }
    
    def main(args: Array[String])
    {
        val template: SparkTemplate = new DefaultSparkTemplate()
        template.execute { sc => do_execute(sc) }
    }
    
    def do_execute(sc: SparkContext)
    {
        val files:List[String] = List("station_5min/d11_text_station_5min_2010_01_*.txt.gz")
        val m_string_rdd:RDD[String] = MLibUtils.new_rdd(sc, files, 4)
     
        val handler:PivotHandler = new StandardPivotHandler(sc, Fields.Speed)
        val m_vector_rdd:RDD[Vector] = MLibUtils.pivot(m_string_rdd, handler)
        
        //val files:List[String] = List("/Users/johngill/Documents/DSE/jgilliii/dse_capstone/ml/notebooks/t_data.csv")
        //val m_vector_rdd:RDD[Vector] = MLibUtils.new_rdd(sc, files, 4).map(parseVector)
        
        val mahala:AnomalyDetector = new MahalanobisOutlier(sc)
        mahala.fit(m_vector_rdd)
        mahala.DetectOutlier(m_vector_rdd.zipWithIndex(), 3.0).collect().foreach(println)
        
        val kmeans:AnomalyDetector = new KMeansOutlier(sc, 10, 20)
        kmeans.fit(m_vector_rdd)
        kmeans.DetectOutlier(m_vector_rdd.zipWithIndex(), 4.5).collect().foreach(println)
    }
}