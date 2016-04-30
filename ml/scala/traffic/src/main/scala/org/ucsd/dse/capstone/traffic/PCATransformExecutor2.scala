package org.ucsd.dse.capstone.traffic

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.SQLUserDefinedType

import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }

/**
 * Defines the classes and logic that transforms original space into new space. Result is stored as CSV files in the location specified by S3Parameter and OutputParameter.
 *
 * @author dyerke
 */
class PCATransformExecutor2(m_pair_rdd: RDD[(Array[Int], Vector)], parameter: PCATransformParameter) extends Executor[Array[Vector]] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): Array[Vector] = {
    do_execute(sc, m_pair_rdd, parameter)
  }

  private def do_execute(sc: SparkContext, m_pair_rdd: RDD[(Array[Int], Vector)], parameter: PCATransformParameter): Array[Vector] = {
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
    IOUtils.dump_vec_to_output(trans_vectors_arr, "transformed", parameter.m_output_param)
    //
    trans_vectors_arr
  }
}