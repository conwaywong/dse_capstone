package org.ucsd.dse.capstone.traffic

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.SQLUserDefinedType

/**
 * Defines the logic that executes PCA against a specified compressed RDD[Row]. The
 * result of the PCA is stored as CSV files in the location specified by S3Parameter and OutputParameter.
 *
 * @author dyerke
 */
class PCAExecutor(pivot_df: DataFrame, output_param: OutputParameter, k: Int = 5, log_output: Boolean = true) extends Executor[PCAResults] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): PCAResults = {
    //
    // Execute PCA for total flow (weekday and weekend)
    //
    val result_list: List[RDD[(Array[Int], Vector)]] = IOUtils.get_total_flow_rdd_partitions(sc, pivot_df)
    //
    // execute TOTAL_FLOW weekday PCA
    //
    val weekday_rdd: RDD[Vector] = result_list(0).map(_._2)
    val weekday_pca_result = do_execute(weekday_rdd, "weekday", TOTAL_FLOW, output_param)
    //
    // execute TOTAL_FLOW weekend PCA
    //
    val weekend_rdd: RDD[Vector] = result_list(1).map(_._2)
    val weekend_pca_result = do_execute(weekend_rdd, "weekend", TOTAL_FLOW, output_param)
    //
    new PCAResults(weekday_pca_result, weekend_pca_result)
  }

  private def do_execute(m_vector_rdd: RDD[Vector], token_name: String, obs_enum: PivotColumn, output_param: OutputParameter): PCAResult = {
    //
    // calculate summary stats
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    //
    // obtain mean vector
    //
    val mean_vector = m_summary_stats.mean
    IOUtils.dump_vec_to_output(List(mean_vector), token_name + "_mean_vector", output_param)
    //
    // execute PCA
    //
    val (eigenvectors, eigenvalues) = MLibUtils.execute_pca(m_vector_rdd, k)
    //
    // eigenvectors written out as column-major matrix, eigenvalues written out as one row
    //
    IOUtils.dump_mat_to_output(eigenvectors, token_name + "_eigenvectors", output_param)
    IOUtils.dump_vec_to_output(List(eigenvalues), token_name + "_eigenvalues", output_param)
    if (log_output) {
      //
      // print statements to verify
      //
      println("eigenvectors= " + eigenvectors)
      println("eigenvalues= " + eigenvalues)
      val m_list_buffer = new ListBuffer[Double]()
      val m_eig_arr: Array[Double] = eigenvalues.toArray
      var cum_sum = 0.0
      for (i <- 0 to m_eig_arr.length - 1) {
        cum_sum += m_eig_arr(i)
        m_list_buffer += cum_sum
      }
      println("perc variance explained= " + m_list_buffer)
    }
    //
    new PCAResult(eigenvectors, eigenvalues, mean_vector)
  }
}