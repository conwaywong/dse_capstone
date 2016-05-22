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
    //    val station_ids: Array[Int] = Array(2351, 2322, 2479, 15683, 17240, 17241, 17252)
    val station_id_range: Array[Int] = Array(340, 39923)
    val path = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/parquet_2008"
    val pivot_df: DataFrame = IOUtils.read_pivot_df2(sqlContext, path)
    pivot_df.sample(false, 0.01).collect().foreach(println)
    //
    val rdd_results: List[RDD[(Array[Int], Vector)]] = IOUtils.get_total_flow_rdd_partitions(sc, pivot_df)
    val op = new OutputParameter("station_extract", "/var/tmp/stationextract")
    //
    //    val output_aws_id = null // replace with access id
    //    val output_aws_secret_key = null // replace with secret key
    //    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
    //    val client: AmazonS3 = new AmazonS3Client(cred)
    //    val bucket_name: String = "dse-team2-2014"
    //    val s3_param = new S3Parameter(client, bucket_name)
    //
    //    val executor: Executor[PCAResults] = new PCAExecutor(paths, output_parameter, s3_param)
    val executor: Executor[_] = new StationExtractExecutor(rdd_results(0), station_id_range, op)
    executor.execute(sc, sqlContext)
  }
}
