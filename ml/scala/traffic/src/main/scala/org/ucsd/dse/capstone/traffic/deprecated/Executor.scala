package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Trait defining signature for executors that will perform work in Spark using the 
 * specified SparkContext and SQLContext
 * 
 * @author dyerke
 */
trait Executor[T] {

  def execute(sc: SparkContext, sql_context: SQLContext, args: String*): T
}