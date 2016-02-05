package org.ucsd.dse.capstone.traffic

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import com.amazonaws.services.s3.AmazonS3

/**
 * @author dyerke
 */
trait RDDCreator {

  def new_rdd(sc: SparkContext, client: AmazonS3): RDD[Vector]
}