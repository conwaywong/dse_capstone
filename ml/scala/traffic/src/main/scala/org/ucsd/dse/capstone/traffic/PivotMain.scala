package org.ucsd.dse.capstone.traffic

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Driver that executes the pivot of traffic data from RDD[String] to RDD[Row] and serializes
 * it to compressed text.
 * 
 * @author dyerke
 */
object PivotMain extends Logging {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val files: List[String] = List("/home/dyerke/Documents/DSE/capstone_project/traffic/data/d11_text_station_5min_2015_01_01_mod.txt")
    val output_dir = "/tmp/test_output2"
    //
    val pivot_executor: Executor[_] = new PivotExecutor(files, output_dir)
    pivot_executor.execute(sc, sqlContext)
  }
}