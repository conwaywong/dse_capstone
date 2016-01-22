package org.ucsd.dse.capstone.traffic

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD

/**
 * @author dyerke
 */
object PCAMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {

    val files: List[String] = List("/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010", "/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010_first_seven_days")
    val m_string_rdd: RDD[String] = MLibUtils.new_rdd(sc, files, 4)

    //
    // Execute PCA for each field
    //
    val m_fields_pca = List[Tuple2[String, Int]](
      ("/tmp/total_flow.", Fields.TotalFlow),
      ("/tmp/occupancy.", Fields.Occupancy),
      ("/tmp/speed.", Fields.Speed))
    val fid = "01_2010" // hardcode id for now
    m_fields_pca.foreach { tuple: Tuple2[String, Int] =>
      val file_dir_prefix = tuple._1
      val pivot_field = tuple._2
      do_run(sc, m_string_rdd, fid, file_dir_prefix, pivot_field)
    }
  }

  private def do_run(sc: SparkContext, m_string_rdd: RDD[String], fid: String, file_dir_prefix: String, pivot_field: Int) = {
    val handler: PivotHandler = new StandardPivotHandler(sc, pivot_field)
    val m_vector_rdd: RDD[Vector] = MLibUtils.pivot(m_string_rdd, handler)
    //
    // obtain mean vector
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename = file_dir_prefix + "mean_vector." + fid + ".csv"
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
    val eigenvectors_filename = file_dir_prefix + "eigenvectors." + fid + ".csv"
    MLibUtils.write_matrix(eigenvectors_filename, eigenvectors)
    //
    // eigenvalues written out as one row
    //
    val eigenvalue_filename = file_dir_prefix + "eigenvalues." + fid + ".csv"
    MLibUtils.write_vectors(eigenvalue_filename, List[Vector](eigenvalues))
    //
    // take a sample of 10 vectors
    //
    val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
    val sample_filename = file_dir_prefix + "samples." + fid + ".csv"
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