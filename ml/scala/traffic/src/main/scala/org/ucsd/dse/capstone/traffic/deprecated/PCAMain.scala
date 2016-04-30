package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Driver that executes PCA against a compressed RDD[Row]
 *
 * @author dyerke
 */
object PCAMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val path = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/parquet"
    //    val output_parameter = new OutputParameter("test", "output")
    val output_parameter = new OutputParameter("test", "/var/tmp/test_results/output2")
    //
    //    val output_aws_id = null // replace with access id
    //    val output_aws_secret_key = null // replace with secret key
    //    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
    //    val client: AmazonS3 = new AmazonS3Client(cred)
    //    val bucket_name: String = "dse-team2-2014"
    //    val s3_param = new S3Parameter(client, bucket_name)
    //
    //    val executor: Executor[PCAResults] = new PCAExecutor(paths, output_parameter, s3_param)
    val pivot_df: DataFrame = IOUtils.read_pivot_df2(sqlContext, path)
    val executor: Executor[PCAResults] = new PCAExecutor(pivot_df, output_parameter)
    executor.execute(sc, sqlContext)
  }
}
