package org.ucsd.dse.capstone.traffic

import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client

/**
 * @author dyerke
 */
object PCAMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    val output_aws_id = null // replace with access id
    val output_aws_secret_key = null // replace with secret key
    val cred: AWSCredentials = new BasicAWSCredentials(output_aws_id, output_aws_secret_key)
    val client: AmazonS3 = new AmazonS3Client(cred)
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    //
    // Read DataFrame
    //
    val sqlContext: SQLContext = new SQLContext(sc)
    val path = "/var/tmp/test_output2"
    val pivot_df = IOUtils.read_pivot_df(sc, sqlContext, path)
    //
    // Execute PCA for each field
    //
    val m_column_prefixes = List(PivotColumnPrefixes.TOTAL_FLOW, PivotColumnPrefixes.SPEED, PivotColumnPrefixes.OCCUPANCY)
    val fid = "test"
    val file_dir_prefix = "/var/tmp/";
    m_column_prefixes.foreach { column_prefix =>
      val m_vector_rdd: RDD[Vector] = IOUtils.toVectorRDD(pivot_df, column_prefix)
      execute(m_vector_rdd, fid, file_dir_prefix)
    }
  }

  private def execute(m_vector_rdd: RDD[Vector], fid: String, file_dir_prefix: String) = {
    //
    // obtain mean vector
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename = file_dir_prefix + "mean_vector." + fid + ".csv"
    val mean_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_vectors(mean_filename, List[Vector](Vectors.dense(mean_vector)), filename => {
      new BufferedWriter(new OutputStreamWriter(mean_stream))
    })
    val mean_stream_tuple = (mean_filename, mean_stream)
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
    IOUtils.write_matrix(eigenvectors_filename, eigenvectors, filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvector_stream))
    })
    val eigenvector_stream_tuple = (eigenvectors_filename, eigenvector_stream)
    //
    // eigenvalues written out as one row
    //
    val eigenvalue_filename = file_dir_prefix + "eigenvalues." + fid + ".csv"
    val eigenvalue_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    IOUtils.write_vectors(eigenvalue_filename, List[Vector](eigenvalues), filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvalue_stream))
    })
    val eigenvalue_stream_tuple = (eigenvalue_filename, eigenvalue_stream)
    //
    // take a sample of 10 vectors
    //
    val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
    val sample_filename = file_dir_prefix + "samples." + fid + ".csv"
    val sample_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    IOUtils.write_vectors(sample_filename, sample_arr, filename => {
      new BufferedWriter(new OutputStreamWriter(sample_stream))
    })
    val sample_stream_tuple = (sample_filename, sample_stream)
    //
    // write streams to files
    //
    val tuple_list: List[Tuple2[String, ByteArrayOutputStream]] = List[Tuple2[String, ByteArrayOutputStream]](mean_stream_tuple, eigenvector_stream_tuple, eigenvalue_stream_tuple, sample_stream_tuple)
    tuple_list.foreach { tuple: Tuple2[String, ByteArrayOutputStream] =>
      val filename: String = tuple._1
      val stream: ByteArrayOutputStream = tuple._2
      //
      process_stream(filename, stream)
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

  //  private def process_stream(client: AmazonS3, filename: String, stream: ByteArrayOutputStream): Unit = {
  //    val output_bucket_name = "dse-team2-2014"
  //    val simple_filename = filename.split("/").last
  //    val output_bucket_key = "output/" + simple_filename
  //    //
  //    val bytes: Array[Byte] = stream.toByteArray();
  //    //
  //    val in_stream: InputStream = new ByteArrayInputStream(bytes)
  //    val in_stream_meta: ObjectMetadata = new ObjectMetadata()
  //    in_stream_meta.setContentLength(bytes.length)
  //    //
  //    println("Invoking client.putObject with parameters: %s,%s".format(output_bucket_name, output_bucket_key))
  //    client.putObject(output_bucket_name, output_bucket_key, in_stream, in_stream_meta)
  //  }

  private def process_stream(filename: String, stream: ByteArrayOutputStream): Unit = {
    val out: OutputStream = new BufferedOutputStream(new FileOutputStream(new File(filename)))
    try {
      out.write(stream.toByteArray())
    } finally {
      out.close()
    }
  }
}