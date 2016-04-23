package org.ucsd.dse.capstone.traffic.deprecated

import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
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
class PCATransformExecutor2(pivot_df: DataFrame, parameter: PCATransformParameter) extends Executor[Tuple2[Array[Vector], String]] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): Tuple2[Array[Vector], String] = {
    val m_pair_rdd: RDD[(Array[Int], Vector)] = IOUtils.toVectorRDD_withKeys(pivot_df, parameter.m_column)
    do_execute(sc, m_pair_rdd, parameter)
  }

  private def do_execute(sc: SparkContext, m_pair_rdd: RDD[(Array[Int], Vector)], parameter: PCATransformParameter): Tuple2[Array[Vector], String] = {
    val filename_prefix = IOUtils.get_col_prefix(parameter.m_column)
    val fid = parameter.m_output_param.m_output_fid
    val output_dir = parameter.m_output_param.m_output_dir
    val s3_param = parameter.m_output_param.m_s3_param
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
    val trans_vector_rdd: RDD[Vector] = m_pair_rdd.map { pair: (Array[Int], Vector) =>
      val key_fields: Array[Int] = pair._1
      val vec: Vector = pair._2
      //
      val m_breeze_mean_vector: BDV[Double] = broadcast_breeze_mean_vector.value
      val m_top_eigen: Matrix = broadcast_top_eigen.value
      //
      // subtract mean, project onto top eigen
      //
      val current_vector: BDV[Double] = MLibUtils.toBreeze(vec)
      current_vector -= m_breeze_mean_vector
      val trans_vec = top_eigen.transpose.multiply(MLibUtils.fromBreeze(current_vector))
      //
      // construct result
      //
      val d_arr = trans_vec.toArray
      val result_list: ListBuffer[Double] = new ListBuffer[Double]()
      for (key <- key_fields) {
        result_list += key.toDouble
      }
      result_list.appendAll(d_arr)
      Vectors.dense(result_list.toArray)
    }
    val trans_vectors_arr: Array[Vector] = trans_vector_rdd.collect()
    val trans_vectors_filename = output_dir + filename_prefix + "transformed." + fid + ".csv"
    val trans_vectors_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_vectors(trans_vectors_filename, trans_vectors_arr, filename => {
      new BufferedWriter(new OutputStreamWriter(trans_vectors_stream))
    })
    //
    if (s3_param != null) {
      IOUtils.process_stream(s3_param.m_client, s3_param.m_bucket_name, output_dir, trans_vectors_filename, trans_vectors_stream)
    } else {
      IOUtils.process_stream(output_dir, trans_vectors_filename, trans_vectors_stream)
    }
    (trans_vectors_arr, trans_vectors_filename)
  }
}