package org.ucsd.dse.capstone.traffic

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Driver that executes PCA, PCA transform, and aggregates eigenvector coefficients
 *
 * @author dyerke
 */
object StationExtractMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val z_list: List[(Array[Int], String, String)] = List(
      (Array(1204825, 1205493, 1204384), "/home/dyerke/Documents/DSE/capstone_project/traffic/data/parquet_2008", "station_extract_2008"))
    z_list.foreach { m_tuple: (Array[Int], String, String) =>
      val station_ids = m_tuple._1
      val path = m_tuple._2
      val token = m_tuple._3
      //
      val pivot_df: DataFrame = IOUtils.read_pivot_df2(sqlContext, path)
      //
      val rdd_results: List[RDD[(Array[Int], Vector)]] = IOUtils.get_total_flow_rdd_partitions(sc, pivot_df)
      val op = new OutputParameter(token, "/var/tmp/stationextract")
      //
      //    val output_aws_id = null // replace with access id
      //    val output_aws_secret_key = null // replace with secret key
      //    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
      //    val client: AmazonS3 = new AmazonS3Client(cred)
      //    val bucket_name: String = "dse-team2-2014"
      //    val s3_param = new S3Parameter(client, bucket_name)
      //
      //    val executor: Executor[PCAResults] = new PCAExecutor(paths, output_parameter, s3_param)
      val executor: Executor[_] = new StationExtractExecutor(rdd_results(0), station_ids, op)
      executor.execute(sc, sqlContext)
    }

  }
}
