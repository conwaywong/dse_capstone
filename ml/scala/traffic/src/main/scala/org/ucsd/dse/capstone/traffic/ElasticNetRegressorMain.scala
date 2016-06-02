package org.ucsd.dse.capstone.traffic

import java.io.FileWriter

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import au.com.bytecode.opencsv.CSVWriter

/**
 * Driver that executes ElasticNetRegressor
 *
 * @author dyerke
 */
object ElasticNetRegressorMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val partitions: List[String] = List("wkday", "wkend")
    val years: List[Int] = List(2008, 2009, 2010, 2011, 2013, 2014)
    //
    for (p <- partitions) {
      for (y <- years) {
        val path = s"/home/dyerke/Documents/DSE/kdyer/dse_capstone_final/dse_capstone/final/data/regression/preprocessed_${y}_${p}.csv"
        val output_path = s"/var/tmp/elastic_net_${y}_${p}_results.csv"
        //
        val executor: Executor[RegressorResult] = new ElasticNetRegressorExecutor(path)
        val result: RegressorResult = executor.execute(sc, sqlContext)
        //
        println(s"For partition ${p}, year ${y}; result=$result")
        //
        val writer: CSVWriter = new CSVWriter(new FileWriter(output_path))
        try {
          result.m_feature_importance.foreach { a: Tuple2[String, Double] =>
            writer.writeNext(Array[String](a._1, a._2.toString))
          }
          writer.writeNext(Array[String](result.m_r2score.toString))
        } finally {
          writer.close()
        }
      }
    }
  }
}
