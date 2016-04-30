package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * An instance of SparkTemplate that submits the Spark Application programatically (without using spark-submit command line)
 * 
 * @author dyerke
 */
class StandaloneSparkTemplate(app_name: String = "SparkApp", hostname: String = "192.168.0.10", spark_home: String = "/usr/local/spark-1.6.0-bin-hadoop2.6") extends SparkTemplate {

  val m_app_name = app_name
  val m_hostname = hostname
  val m_spark_home = spark_home

  def execute(f: (SparkContext) => Unit) = {
    val conf = new SparkConf()
    conf.setAppName(m_app_name)
    conf.setMaster("local[4]")
    conf.setSparkHome(m_spark_home)
    conf.set("spark.driver.host", m_hostname)

    val sc = new SparkContext(conf)
    //
    f(sc)
    //
    sc.stop()
  }
}