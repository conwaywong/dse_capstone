package org.ucsd.dse.capstone.traffic

import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }

/**
 * Defines the classes and logic that transforms original space into new space. Result is stored as CSV files in the location specified by S3Parameter and OutputParameter.
 *
 * @author dyerke
 */
class PCATransformExecutor(parameters: List[PCATransformParameter], paths: List[String]) extends Executor[Unit] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*) = {
    //
    // Read DataFrame
    //
    val pivot_csv_path = paths.mkString(",")
    val pivot_df: DataFrame = IOUtils.read_pivot_df(sc, sql_context, pivot_csv_path)
    //
    // Execute PCA Transform for each parameter
    //
    parameters.foreach { parameter =>
      val m_vector_rdd: RDD[Vector] = IOUtils.toVectorRDD(pivot_df, parameter.m_column)
      do_execute(sc, m_vector_rdd, parameter)
    }
  }

  private def do_execute(sc: SparkContext, m_vector_rdd: RDD[Vector], parameter: PCATransformParameter) = {
    val filename_prefix = IOUtils.get_col_prefix(parameter.m_column)
    val fid = parameter.m_output_param.m_output_fid
    val output_dir = parameter.m_output_param.m_output_dir
    val s3_param= parameter.m_output_param.m_s3_param
    //
    // execute transform
    //
    val breeze_mean_vector: BDV[Double] = MLibUtils.toBreeze(parameter.m_mean)
    val breeze_eigenvectors: BDM[Double] = MLibUtils.toBreeze(parameter.m_eigenvectors)
    val top_eigen: Matrix = MLibUtils.fromBreeze(breeze_eigenvectors(::, 0 to (parameter.m_k - 1)))
    //
    val broadcast_breeze_mean_vector = sc.broadcast(breeze_mean_vector)
    val broadcast_top_eigen = sc.broadcast(top_eigen)
    //
    val trans_vector_rdd: RDD[Vector] = m_vector_rdd.map { vec =>
      val m_breeze_mean_vector: BDV[Double] = broadcast_breeze_mean_vector.value
      val m_top_eigen: Matrix = broadcast_top_eigen.value
      //
      // subtract mean, project onto top eigen
      //
      val current_vector: BDV[Double] = MLibUtils.toBreeze(vec)
      current_vector -= m_breeze_mean_vector
      top_eigen.transpose.multiply(MLibUtils.fromBreeze(current_vector))
    }
    val trans_vector_arr: Array[Vector] = trans_vector_rdd.collect()
    val trans_vector_filename = output_dir + filename_prefix + "transformed." + fid + ".csv"
    val trans_vector_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_vectors(trans_vector_filename, trans_vector_arr, filename => {
      new BufferedWriter(new OutputStreamWriter(trans_vector_stream))
    })
    //
    if (s3_param != null) {
      IOUtils.process_stream(s3_param.m_client, s3_param.m_bucket_name, output_dir, trans_vector_filename, trans_vector_stream)
    } else {
      IOUtils.process_stream(output_dir, trans_vector_filename, trans_vector_stream)
    }
  }
}