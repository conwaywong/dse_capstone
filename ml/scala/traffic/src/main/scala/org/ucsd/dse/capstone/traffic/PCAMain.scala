package org.ucsd.dse.capstone.traffic

import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileWriter
import java.io.OutputStreamWriter
import java.io.Writer

import scala.collection.mutable.ListBuffer

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.SQLUserDefinedType

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
    val eigenvector_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    MLibUtils.write_matrix(eigenvectors_filename, eigenvectors, filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvector_stream))
    })
    val eigenvector_stream_tuple = (eigenvectors_filename, eigenvector_stream)
    //
    // eigenvalues written out as one row
    //
    val eigenvalue_filename = file_dir_prefix + "eigenvalues." + fid + ".csv"
    val eigenvalue_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    MLibUtils.write_vectors(eigenvalue_filename, List[Vector](eigenvalues), filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvalue_stream))
    })
    val eigenvalue_stream_tuple = (eigenvalue_filename, eigenvalue_stream)
    //
    // take a sample of 10 vectors
    //
    val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
    val sample_filename = file_dir_prefix + "samples." + fid + ".csv"
    val sample_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    MLibUtils.write_vectors(sample_filename, sample_arr, filename => {
      new BufferedWriter(new OutputStreamWriter(sample_stream))
    })
    val sample_stream_tuple = (sample_filename, sample_stream)
    //
    // write streams to files
    //
    val tuple_list: List[Tuple2[String, ByteArrayOutputStream]] = List[Tuple2[String, ByteArrayOutputStream]](eigenvector_stream_tuple, eigenvalue_stream_tuple, sample_stream_tuple)
    tuple_list.foreach { tuple: Tuple2[String, ByteArrayOutputStream] =>
      val filename: String = tuple._1
      val stream: ByteArrayOutputStream = tuple._2
      //
      val writer: Writer = new FileWriter(new File(filename))
      try {
        IOUtils.write(stream.toByteArray(), writer)
      } finally {
        IOUtils.closeQuietly(writer)
      }
    }
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