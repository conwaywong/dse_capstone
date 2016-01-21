package org.ucsd.dse.capstone.traffic.support

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Defines a trait outlining the signature of PivotHandlers responsible for pivoting the traffic dataset.
 *
 * @author dyerke
 */
trait PivotHandler {

  def pivot(m_string_rdd: RDD[String]): RDD[Vector]
}