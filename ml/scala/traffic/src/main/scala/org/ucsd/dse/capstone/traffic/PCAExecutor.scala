package org.ucsd.dse.capstone.traffic

import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ListBuffer

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.SQLUserDefinedType

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata

/**
 * @author dyerke
 */
class PCAResult(
    eigenvectors: Tuple2[Matrix, String],
    eigenvalues: Tuple2[Vector, String],
    meanvector: Tuple2[Array[Double], String],
    stdvector: Tuple2[Array[Double], String],
    samples: Tuple2[Array[Vector], String]) {
  val m_eig = eigenvectors
  val m_eig_values = eigenvalues
  val m_mean_vec = meanvector
  val m_std_vec = stdvector
  val m_samples = samples
}

class PCAResults(total_flow: PCAResult, speed: PCAResult, occupancy: PCAResult) {
  val m_total_flow = total_flow
  val m_speed = speed
  val m_occupancy = occupancy
}

class S3Parameter(client: AmazonS3, bucket_name: String) {
  val m_client = client
  val m_bucket_name = bucket_name
}

class OutputParameter(output_fid: String, output_dir: String) {
  val m_output_fid = output_fid
  val m_output_dir = FilenameUtils.normalizeNoEndSeparator(output_dir + "/").concat("/")
}

class PCAExecutor(paths: List[String], output_param: OutputParameter, s3_param: S3Parameter = null, k: Int = 30, log_output: Boolean = true) extends Executor[PCAResults] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): PCAResults = {
    //
    // Read DataFrame
    //
    val csv_path = paths.mkString(",")
    val pivot_df: DataFrame = MLibUtils.read_pivot_df(sc, sql_context, csv_path)
    //
    // Execute PCA for each field
    //
    val m_column_prefixes = List(PivotColumnPrefixes.TOTAL_FLOW, PivotColumnPrefixes.SPEED, PivotColumnPrefixes.OCCUPANCY)
    val results: ListBuffer[PCAResult] = new ListBuffer[PCAResult]()
    m_column_prefixes.foreach { column_prefix =>
      val m_vector_rdd: RDD[Vector] = MLibUtils.to_vector_rdd(pivot_df, column_prefix)
      results += do_execute(m_vector_rdd, output_param.m_output_fid, output_param.m_output_dir)
    }
    new PCAResults(results(0), results(1), results(2))
  }

  private def do_execute(m_vector_rdd: RDD[Vector], fid: String, output_dir: String): PCAResult = {
    //
    // calculate summary stats
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    //
    // obtain mean vector
    //
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename = output_dir + "mean_vector." + fid + ".csv"
    val mean_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    MLibUtils.write_vectors(mean_filename, List[Vector](Vectors.dense(mean_vector)), filename => {
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
    val std_filename = output_dir + "std_vector." + fid + ".csv"
    val std_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    MLibUtils.write_vectors(std_filename, List[Vector](Vectors.dense(std_vector)), filename => {
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
    val eigenvectors_filename = output_dir + "eigenvectors." + fid + ".csv"
    val eigenvectors_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    MLibUtils.write_matrix(eigenvectors_filename, eigenvectors, filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvectors_stream))
    })
    val eigenvectors_stream_tuple = (eigenvectors_filename, eigenvectors_stream)
    //
    // eigenvalues written out as one row
    //
    val eigenvalues_filename = output_dir + "eigenvalues." + fid + ".csv"
    val eigenvalues_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    MLibUtils.write_vectors(eigenvalues_filename, List[Vector](eigenvalues), filename => {
      new BufferedWriter(new OutputStreamWriter(eigenvalues_stream))
    })
    val eigenvalues_stream_tuple = (eigenvalues_filename, eigenvalues_stream)
    //
    // take a sample of 10 vectors
    //
    val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
    val sample_filename = output_dir + "samples." + fid + ".csv"
    val sample_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    MLibUtils.write_vectors(sample_filename, sample_arr, filename => {
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
        process_stream(s3_param.m_client, s3_param.m_bucket_name, output_param.m_output_dir, filename, stream)
      } else {
        process_stream(output_param.m_output_dir, filename, stream)
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

  private def process_stream(client: AmazonS3, bucket_name: String, output_dir: String, filename: String, stream: ByteArrayOutputStream): Unit = {
    val output_bucket_name = bucket_name
    val simple_filename = filename.split("/").last
    val output_bucket_key = simple_filename
    //
    val bytes: Array[Byte] = stream.toByteArray();
    //
    val in_stream: InputStream = new ByteArrayInputStream(bytes)
    val in_stream_meta: ObjectMetadata = new ObjectMetadata()
    in_stream_meta.setContentLength(bytes.length)
    //
    println("Invoking client.putObject with parameters: %s,%s".format(output_bucket_name, output_bucket_key))
    client.putObject(output_bucket_name, output_bucket_key, in_stream, in_stream_meta)
  }

  private def process_stream(output_dir: String, filename: String, stream: ByteArrayOutputStream): Unit = {
    val dir = new File(output_dir)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val out: OutputStream = new BufferedOutputStream(new FileOutputStream(new File(filename)))
    try {
      out.write(stream.toByteArray())
    } finally {
      out.close()
    }
  }
}