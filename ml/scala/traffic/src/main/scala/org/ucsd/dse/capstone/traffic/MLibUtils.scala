package org.ucsd.dse.capstone.traffic

import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.io.Writer
import java.util.Arrays

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import au.com.bytecode.opencsv.CSVWriter
import breeze.io.{ CSVWriter => BCSV }
import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.{ svd => brzSvd }

object PivotColumnPrefixes {
  val TOTAL_FLOW = 1
  val SPEED = 2
  val OCCUPANCY = 3
}
/**
 * Utility functions wrapping Spark MLib APIs.
 *
 * @author dyerke
 */
object MLibUtils {

  /**
   * new FileWriter(new File(filename))
   * Returns summary stats for specified RDD[Vector]
   *
   * @param rdd
   * @return MultivariateStatisticalSummary
   */
  def summary_stats(rdd: RDD[Vector]): MultivariateStatisticalSummary = {
    Statistics.colStats(rdd)
  }

  /**
   * Computes the top k principal components and a vector of proportions of
   * variance explained by each principal component.
   * Rows correspond to observations and columns correspond to variables.
   * The principal components are stored a local matrix of size n-by-k.
   * Each column corresponds for one principal component,
   * and the columns are in descending order of component variance.
   * The row data do not need to be "centered" first; it is not necessary for
   * the mean of each column to be 0.
   *
   * Note that this cannot be computed on matrices with more than 65535 columns.
   *
   * @param m_vector_rdd the RDD[Vector] to execute PCA against
   * @param k number of principal components to use
   * @return a matrix of size n-by-k, whose columns are principal components, and
   * a vector of values which indicate how much variance each principal component
   * explains
   */
  def execute_pca(m_vector_rdd: RDD[Vector], k: Int): (Matrix, Vector) = {
    val row_matrix: RowMatrix = new RowMatrix(m_vector_rdd)
    val n = row_matrix.numCols().toInt
    require(k > 0 && k <= n, s"k = $k out of range (0, n = $n]")

    val Cov: BDM[Double] = toBreeze(row_matrix.computeCovariance())

    val brzSvd.SVD(u: BDM[Double], s: BDV[Double], _) = brzSvd(Cov)

    val eigenSum = s.data.sum
    val explainedVariance = s.data.map(_ / eigenSum)

    if (k == n) {
      (Matrices.dense(n, k, u.data), Vectors.dense(explainedVariance))
    } else {
      (Matrices.dense(n, k, Arrays.copyOfRange(u.data, 0, n * k)),
        Vectors.dense(Arrays.copyOfRange(explainedVariance, 0, k)))
    }
  }

  def toBreeze(m_matrix: Matrix): BDM[Double] = {
    if (!m_matrix.isTransposed) {
      new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray)
    } else {
      val breezeMatrix = new BDM[Double](m_matrix.numCols, m_matrix.numRows, m_matrix.toArray)
      breezeMatrix.t
    }
  }

  def read_pivot_df(sc: SparkContext, sqlContext: SQLContext, path: String, table_name: String = "pivot_all"): DataFrame = {
    // Create Schema
    val (schema, m_observation_times) = get_schema()
    // create DataFrame
    val m_string_rdd: RDD[String] = sc.textFile(path)
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
    pivot_df
  }

  def to_vector_rdd(pivot_df: DataFrame, column_prefix_enum: Int): RDD[Vector] = {
    val column_prefix = column_prefix_enum match {
      case PivotColumnPrefixes.TOTAL_FLOW => "total_flow_"
      case PivotColumnPrefixes.SPEED      => "speed_"
      case PivotColumnPrefixes.OCCUPANCY  => "occupancy_"
    }
    val (schema, m_observation_times) = get_schema()
    //
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

  private def get_schema(): (StructType, IndexedSeq[String]) = {
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
    (schema, m_observation_times)
  }

  /**
   * Write the specified vectors into CSV file specified by filename.
   *
   * @param filename file to write to
   * @param m_list TraversableOnce[Vector]
   */
  def write_vectors(filename: String, m_list: TraversableOnce[Vector]): Unit = {
    write_vectors(filename, m_list, filename => {
      new FileWriter(new File(filename))
    })
  }

  def write_vectors(filename: String, m_list: TraversableOnce[Vector], new_writer: (String) => Writer): Unit = {
    val csv_writer: CSVWriter = new CSVWriter(new_writer(filename))
    try {
      try {
        m_list.foreach { a_vec: Vector =>
          val m_vec = a_vec.toArray
          val m_string_vec: Array[String] = new Array[String](m_vec.length)
          for (i <- 0 to m_vec.length - 1) {
            m_string_vec(i) = m_vec(i).toString()
          }
          csv_writer.writeNext(m_string_vec)
        }
      } finally {
        csv_writer.close()
      }
    } catch {
      case e: IOException => throw new IllegalStateException(e)
    }
  }

  def write_matrix(filename: String, m_matrix: Matrix): Unit = {
    // written out as column major matrix
    write_matrix(filename, m_matrix, filename => {
      new FileWriter(new File(filename))
    })
  }

  def write_matrix(filename: String, m_matrix: Matrix, new_writer: (String) => Writer): Unit = {
    // written out as column major matrix
    val mat: BDM[Double] = new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray)
    BCSV.write(new_writer(filename), IndexedSeq.tabulate(mat.rows, mat.cols)(mat(_, _).toString))
  }
}