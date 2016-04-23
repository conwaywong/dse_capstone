package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.SparkContext

/**
 * Defines the helper class, main drivers will utilize to execute work using the SparkContext.
 * Instances of SparkTemplate are responsible to manage the lifecycle for the SparkContext.
 *
 * @author dyerke
 */
trait SparkTemplate {
  def execute(f: (SparkContext) => Unit)
}