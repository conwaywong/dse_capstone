package org.ucsd.dse.capstone.traffic.deprecated

import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Defines the logic that executes PCA against a specified compressed RDD[Row]. The
 * result of the PCA is stored as CSV files in the location specified by S3Parameter and OutputParameter.
 *
 * @author dyerke
 */
class PCAExecutor(pivot_df: DataFrame, output_param: OutputParameter, k: Int = 30, m_column_prefixes: List[PivotColumn] = List(TOTAL_FLOW, SPEED, OCCUPANCY), log_output: Boolean = true) extends Executor[PCAResults] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): PCAResults = {
    //
    // Execute PCA for each field
    //
    val results: ListBuffer[PCAResult] = new ListBuffer[PCAResult]()
    var total_flow_result: PCAResult = null
    var speed_result: PCAResult = null
    var occupancy_result: PCAResult = null
    m_column_prefixes.foreach { column_prefix =>
      val pca_result = do_execute(IOUtils.toVectorRDD(pivot_df, column_prefix), column_prefix, output_param)
      column_prefix match {
        case TOTAL_FLOW => total_flow_result = pca_result
        case SPEED      => speed_result = pca_result
        case OCCUPANCY  => occupancy_result = pca_result
      }
    }
    new PCAResults(total_flow_result, speed_result, occupancy_result)
  }

  private def do_execute(m_vector_rdd: RDD[Vector], obs_enum: PivotColumn, output_param: OutputParameter): PCAResult = {
    val fid: String = output_param.m_output_fid
    val output_dir: String = output_param.m_output_dir
    val s3_param: S3Parameter = output_param.m_s3_param
    val filename_prefix = IOUtils.get_col_prefix(obs_enum)
    //
    // calculate summary stats
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    //
    // obtain mean vector
    //
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename = output_dir + filename_prefix + "mean_vector." + fid + ".csv"
    val mean_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_vectors(mean_filename, List[Vector](Vectors.dense(mean_vector)), filename => {
      new BufferedWriter(new OutputStreamWriter(mean_stream))
    })
    val mean_stream_tuple = (mean_filename, mean_stream)
    //
    // obtain std_vector
    //
    val std_vector = m_summary_stats.variance.toArray
    for (i <- 0 to std_vector.length - 1) {
      std_vector.update(i, Math.sqrt(std_vector(i)))
    }
    val std_filename = output_dir + filename_prefix + "std_vector." + fid + ".csv"
    val std_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_vectors(std_filename, List[Vector](Vectors.dense(std_vector)), filename => {
      new BufferedWriter(new OutputStreamWriter(std_stream))
    })
    val std_stream_tuple = (std_filename, std_stream)
    //
    // execute PCA
    //
    val m_pca_vector_rdd: RDD[Vector] = m_vector_rdd
    val (eigenvectors, eigenvalues) = MLibUtils.execute_pca(m_pca_vector_rdd, k)
    //
    // eigenvectors written out as column-major matrix
    //
    val eigenvectors_filename = output_dir + filename_prefix + "eigenvectors." + fid + ".csv"
    val eigenvectors_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_matrix(eigenvectors_filename, eigenvectors, filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvectors_stream))
    })
    val eigenvectors_stream_tuple = (eigenvectors_filename, eigenvectors_stream)
    //
    // eigenvalues written out as one row
    //
    val eigenvalues_filename = output_dir + filename_prefix + "eigenvalues." + fid + ".csv"
    val eigenvalues_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    IOUtils.write_vectors(eigenvalues_filename, List[Vector](eigenvalues), filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvalues_stream))
    })
    val eigenvalues_stream_tuple = (eigenvalues_filename, eigenvalues_stream)
    //
    // take a sample of 10 vectors
    //
    val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
    val sample_filename = output_dir + filename_prefix + "samples." + fid + ".csv"
    val sample_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    IOUtils.write_vectors(sample_filename, sample_arr, filename => {
      new BufferedWriter(new OutputStreamWriter(sample_stream))
    })
    val sample_stream_tuple = (sample_filename, sample_stream)
    //
    // write streams to files
    //
    val tuple_list: List[Tuple2[String, ByteArrayOutputStream]] = List[Tuple2[String, ByteArrayOutputStream]](mean_stream_tuple, std_stream_tuple, eigenvectors_stream_tuple, eigenvalues_stream_tuple, sample_stream_tuple)
    tuple_list.foreach { tuple: Tuple2[String, ByteArrayOutputStream] =>
      val filename: String = tuple._1
      val stream: ByteArrayOutputStream = tuple._2
      //
      if (s3_param != null) {
        IOUtils.process_stream(s3_param.m_client, s3_param.m_bucket_name, output_param.m_output_dir, filename, stream)
      } else {
        IOUtils.process_stream(output_param.m_output_dir, filename, stream)
      }
    }
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
    val r_eigenvectors: Tuple2[Matrix, String] = (eigenvectors, eigenvectors_filename)
    val r_eigenvalues: Tuple2[Vector, String] = (eigenvalues, eigenvalues_filename)
    val r_meanvector: Tuple2[Array[Double], String] = (mean_vector, mean_filename)
    val r_stdvector: Tuple2[Array[Double], String] = (std_vector, std_filename)
    val r_samples: Tuple2[Array[Vector], String] = (sample_arr, sample_filename)
    new PCAResult(r_eigenvectors, r_eigenvalues, r_meanvector, r_stdvector, r_samples)
  }
}