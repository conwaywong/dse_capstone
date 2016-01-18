package org.ucsd.dse.capstone.traffic

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD

/**
 * @author dyerke
 */
object SparkMLib {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMLibPCA")
    val sc = new SparkContext(conf)
    //
    val m_file_name = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010"
    //val m_file_name = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/d11_text_station_5min_2015_01_01.txt"
    val fid = m_file_name.split('/').last
    //
    val m_vector_rdd: RDD[Vector] = MLibUtils.pivot(sc, m_file_name, 4)
    //
    // obtain mean vector
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename = "/tmp/mean_vector." + fid + ".csv"
    MLibUtils.write_vectors(mean_filename, List[Vector](Vectors.dense(mean_vector)))
    //
    // execute PCA
    //
    val m_pca_vector_rdd: RDD[Vector] = m_vector_rdd
    val k = 30
    val (eigenvectors, eigenvalues) = MLibUtils.execute_pca(m_pca_vector_rdd, k)
    //
    // eigenvectors written out as column-major matrix
    //
    val eigenvectors_filename = "/tmp/eigenvectors." + fid + ".csv"
    MLibUtils.write_matrix(eigenvectors_filename, eigenvectors)
    //
    // eigenvalues written out as one row
    //
    val eigenvalue_filename = "/tmp/eigenvalues." + fid + ".csv"
    MLibUtils.write_vectors(eigenvalue_filename, List[Vector](eigenvalues))
    //
    // take a sample of 10 vectors
    //
    val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
    val sample_filename = "/tmp/samples." + fid + ".csv"
    MLibUtils.write_vectors(sample_filename, sample_arr)
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
}