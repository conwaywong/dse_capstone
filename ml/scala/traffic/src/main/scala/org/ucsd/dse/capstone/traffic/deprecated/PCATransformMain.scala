package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Driver that executes PCA transform against a compressed RDD[Row]
 *
 * @author dyerke
 */
object PCATransformMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val mean_path = "/var/tmp/test_results/output/total_flow_mean_vector.test.csv"
    val eigenvectors_path = "/var/tmp/test_results/output/total_flow_eigenvectors.test.csv"
    //
    val column: PivotColumn = TOTAL_FLOW
    val k: Int = 2
    val mean: DenseVector = IOUtils.read_vectors(mean_path)(0)
    val eigenvectors: DenseMatrix = IOUtils.read_matrix(eigenvectors_path)
    val output_parameter = new OutputParameter("transform", "/var/tmp/transform_results")
    val parameter: PCATransformParameter = new PCATransformParameter(TOTAL_FLOW, mean, eigenvectors, output_parameter, k)
    //
    //    val output_aws_id = null // replace with access id
    //    val output_aws_secret_key = null // replace with secret key
    //    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
    //    val client: AmazonS3 = new AmazonS3Client(cred)
    //    val bucket_name: String = "dse-team2-2014"
    //    val s3_param = new S3Parameter(client, bucket_name)
    //
    //    val executor: Executor[PCAResults] = new PCAExecutor(paths, output_parameter, s3_param)
    val path = "/var/tmp/test_output2"
    val pivot_df: DataFrame = IOUtils.read_pivot_df2(sqlContext, path)
    val executor: Executor[_] = new PCATransformExecutor2(pivot_df, parameter)
    executor.execute(sc, sqlContext)
  }
}
