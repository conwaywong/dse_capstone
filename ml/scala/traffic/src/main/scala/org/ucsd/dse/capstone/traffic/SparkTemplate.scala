package org.ucsd.dse.capstone.traffic

import org.apache.spark.SparkContext

/**
 * @author dyerke
 */
trait SparkTemplate {
  def execute(f: (SparkContext) => Unit)
}