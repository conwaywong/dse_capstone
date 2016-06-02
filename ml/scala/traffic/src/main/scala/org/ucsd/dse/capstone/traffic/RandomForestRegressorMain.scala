package org.ucsd.dse.capstone.traffic

import java.io.FileWriter

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import au.com.bytecode.opencsv.CSVWriter

/**
 * Driver that executes RandomForestRegressor
 *
 * @author dyerke
 */
object RandomForestRegressorMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val path = "/home/dyerke/Documents/DSE/kdyer/dse_capstone_final/dse_capstone/final/data/regression/preprocessed_2008_wkday.csv"
    //
    //
    //    val output_aws_id = null // replace with access id
    //    val output_aws_secret_key = null // replace with secret key
    //    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
    //    val client: AmazonS3 = new AmazonS3Client(cred)
    //    val bucket_name: String = "dse-team2-2014"
    //    val s3_param = new S3Parameter(client, bucket_name)
    //
    //    val executor: Executor[PCAResults] = new PCAExecutor(paths, output_parameter, s3_param)
    val executor: Executor[RegressorResult] = new RandomForestRegressorExecutor(path)
    val result: RegressorResult = executor.execute(sc, sqlContext)
    //
    println(s"result=$result")
    //
    val writer: CSVWriter = new CSVWriter(new FileWriter("/var/tmp/random_forest_2008_wkday_results.csv"))
    try {
      result.m_feature_importance.foreach { a: Tuple2[String, Double] =>
        writer.writeNext(Array[String](a._1, a._2.toString))
      }
      writer.writeNext(Array[String](result.m_r2score.toString))
    } finally {
      writer.close()
    }

  }
}
