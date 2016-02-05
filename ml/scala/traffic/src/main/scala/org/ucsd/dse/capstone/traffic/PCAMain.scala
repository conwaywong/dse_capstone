package org.ucsd.dse.capstone.traffic

import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.io.OutputStreamWriter

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * @author dyerke
 */
object PCAMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    //
    // Read DataFrame
    //
    val sqlContext: SQLContext = new SQLContext(sc)
    val path = "/var/tmp/test_output2"
    val (m_observation_times, pivot_df) = do_read_pivot(sc, sqlContext, path)
    //
    // Execute PCA for each field
    //
    val m_column_prefixes = List("total_flow_", "occupancy_", "speed_")
    val fid = "test"
    val file_dir_prefix = "/var/tmp/";
    m_column_prefixes.foreach { column_prefix =>
      val m_vector_rdd: RDD[Vector] = to_vector_rdd(pivot_df, m_observation_times, column_prefix)
      execute(m_vector_rdd, fid, file_dir_prefix)
    }
  }

  private def do_read_pivot(sc: SparkContext, sqlContext: SQLContext, path: String, table_name: String = "pivot_all", num_partitions: Int = 12): (IndexedSeq[String], DataFrame) = {
    // Create Schema
    var schema = new StructType()

    schema = schema.add(new StructField("station_id", IntegerType))
    schema = schema.add(new StructField("district_id", IntegerType))
    schema = schema.add(new StructField("year", IntegerType))
    schema = schema.add(new StructField("day_of_year", IntegerType))
    schema = schema.add(new StructField("day_of_week", IntegerType))

    val m_obversation_prefixes = List("total_flow", "occupancy", "speed")

    val m_observation_times: IndexedSeq[String] = (0 to 287).map((_ * 5)).map(s => f"${s / 60}%02d${s % 60}%02dM")
    m_obversation_prefixes.foreach { prefix =>
      m_observation_times.foreach { time_str =>
        schema = schema.add(StructField(s"${prefix}_${time_str}", DoubleType))
      }
    }
    // create DataFrame
    val m_string_rdd: RDD[String] = sc.textFile(path, num_partitions)
    val m_row_rdd: RDD[Row] = m_string_rdd.map { s =>
      var s_csv: String = null
      try {
        s_csv = s.substring(1, s.length - 1)
        val s_arr: Array[String] = s_csv.split(",")
        val key_arr: Array[Int] = s_arr.slice(0, 5).map(_.toInt)
        val values_arr: Array[Double] = s_arr.slice(5, s_arr.length).map(_.toDouble)
        Row.fromSeq(List.concat(key_arr, values_arr))
      } catch {
        case e: Exception => throw new IllegalStateException("Error parsing " + s_csv, e)
      }
    }
    val pivot_df: DataFrame = sqlContext.createDataFrame(m_row_rdd, schema)
    // Register the DataFrames as a table.
    pivot_df.registerTempTable(table_name)
    (m_observation_times, pivot_df)
  }

  private def to_vector_rdd(pivot_df: DataFrame, m_observation_times: IndexedSeq[String], column_prefix: String): RDD[Vector] = {
    // Build total_flow column names
    val columns: Seq[Column] = for (i <- m_observation_times) yield new Column(s"${column_prefix}${i}")
    assert(columns.length == 288, { println("Columns is not 288") })

    // create DataFrame which only contains the desired columns
    //
    // Note the casting of the Seq[Column] into varargs
    val subset_df = pivot_df.select(columns: _*)

    // conver to RDD[Vector]
    val rdd_total_flow_rows = subset_df.rdd
    rdd_total_flow_rows.map(x => Vectors.dense(x.toSeq.map(_.asInstanceOf[Double]).toArray))
  }

  private def execute(m_vector_rdd: RDD[Vector], fid: String, file_dir_prefix: String) = {
    //
    // obtain mean vector
    //
    val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename = file_dir_prefix + "mean_vector." + fid + ".csv"
    val mean_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    MLibUtils.write_vectors(mean_filename, List[Vector](Vectors.dense(mean_vector)), filename => {
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

  private def process_stream(filename: String, stream: ByteArrayOutputStream): Unit = {
    val out: OutputStream = new BufferedOutputStream(new FileOutputStream(new File(filename)))
    try {
      out.write(stream.toByteArray())
    } finally {
      out.close()
    }
  }
}