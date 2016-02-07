package org.ucsd.dse.capstone.traffic

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * An instance of SparkTemplate that relies on the command line spark-submit. See PCAMain, PivotMain for example usage.
 * @author dyerke
 */
class DefaultSparkTemplate(app_name: String = "SparkApp") extends SparkTemplate {

  val m_app_name = app_name

  def execute(f: (SparkContext) => Unit) = {
    val conf = new SparkConf()
    conf.setAppName(m_app_name)
    val sc = new SparkContext(conf)
    //
    f(sc)
    //
    sc.stop()
  }
}