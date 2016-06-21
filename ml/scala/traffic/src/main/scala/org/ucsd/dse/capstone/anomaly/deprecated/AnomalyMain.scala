package org.ucsd.dse.capstone.anomaly.deprecated

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ucsd.dse.capstone.traffic._

object AnomalyMain {
  private def parseVector(line: String): Vector =
    {
      Vectors.dense(line.split(',').map(_.toDouble))
    }

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) {
    println("\nStarting Up")
    val sqlContext: SQLContext = new SQLContext(sc)
    val files: List[String] = List("/Users/johngill/Documents/DSE/jgilliii/dse_capstone/ml/scala/traffic/data/d11_text_station_5min_2010_01_*.txt.gz")
    val output_dir = "/tmp/test_output2"

    val pivot_executor: Executor[_] = new PivotExecutor(files, output_dir, false)
    pivot_executor.execute(sc, sqlContext)

    val data_rdd: RDD[(Array[Int], Vector)] = IOUtils.toVectorRDD_withKeys(IOUtils.read_pivot_df(sc, sqlContext, output_dir), TOTAL_FLOW)

    //val files:List[String] = List("/Users/johngill/Documents/DSE/jgilliii/dse_capstone/ml/notebooks/t_data.csv")
    //val data_rdd:RDD[(Array[Int], Vector)] = sc.textFile(files.mkString(",")).zipWithIndex().map{ case(o,i) => (Array(i.toInt), parseVector(o)) }

    val mahala: AnomalyDetector = new MahalanobisOutlier(sc)
    mahala.fit(data_rdd.map { x => x._2 })
    mahala.DetectOutlier(data_rdd, 3.0).collect().foreach { r => println(r._1.deep.mkString(",") + " " + r._2) }

    val kmeans: AnomalyDetector = new KMeansOutlier(sc, 5, 10)
    kmeans.fit(data_rdd.map { x => x._2 })
    kmeans.DetectOutlier(data_rdd, 0.1).collect().foreach { r => println(r._1.deep.mkString(",") + " " + r._2) }
  }
}