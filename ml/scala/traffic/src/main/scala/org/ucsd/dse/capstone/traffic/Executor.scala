package org.ucsd.dse.capstone.traffic

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * @author dyerke
 */
trait Executor[T] {

  def execute(sc: SparkContext, sql_context: SQLContext, args: String*): T
}