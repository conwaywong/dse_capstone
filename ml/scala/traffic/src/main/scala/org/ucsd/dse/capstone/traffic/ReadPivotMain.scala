package org.ucsd.dse.capstone.traffic

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Private
import org.apache.spark.rdd.RDD

/**
 * @author dyerke
 */
object ReadPivotMain extends Logging {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val m_string_rdd: RDD[String] = sc.textFile("/tmp/test_output2", 4)
    // TODO: Logic to create Spark DataFrame
    // TODO: Logic to put into Hive Table in Databricks
    //    var s: String = m_string_rdd.take(1)(0)
    //    println("before, s= " + s)
    //    s= s.replace("[", "")
    //    s= s.replace("]", "")
    //    println("After, s= " + s)
  }
}