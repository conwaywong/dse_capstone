package org.ucsd.dse.capstone.traffic

import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Calendar
import java.util.TimeZone

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.TreeSet

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD

import au.com.bytecode.opencsv.CSVWriter
import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.csvwrite
import breeze.linalg.{ svd => brzSvd }

/**
 * Utility functions wrapping Spark MLib APIs.
 *
 * @author dyerke
 */
object MLibUtils {

  /**
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

  private def toBreeze(m_matrix: Matrix): BDM[Double] = {
    if (!m_matrix.isTransposed) {
      new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray)
    } else {
      val breezeMatrix = new BDM[Double](m_matrix.numCols, m_matrix.numRows, m_matrix.toArray)
      breezeMatrix.t
    }
  }

  /**
   * Pivots the traffic data specified by filename
   *
   * @param sc the SparkContext
   * @param filename filename or directory path to traffic data
   * @param partition_count the number of partitions to cut the dataset into
   * @return RDD[Vector] pivoted by day
   */
  def pivot(sc: SparkContext, filename: String, partition_count: Int = 4): RDD[Vector] = {
    val lines: RDD[String] = sc.textFile(filename, partition_count)
    val t_rdd: RDD[(String, ListBuffer[Any])] = lines.map(m_map_row)
    val pair_rdd: PairRDDFunctions[String, ListBuffer[Any]] = RDD.rddToPairRDDFunctions(t_rdd)
    val m_result_rdd: RDD[(String, Iterable[ListBuffer[Any]])] = pair_rdd.groupByKey()
    m_result_rdd.map(m_map_vector).filter { v => v.size > 0 }
  }

  private def m_map_row(line: String): (String, ListBuffer[Any]) = {
    val x_arr = line.split(",")
    if (x_arr(5) == "ML") {
      //
      val tz = TimeZone.getTimeZone("UTC")
      val fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
      fmt.setTimeZone(tz)
      val key_builder = new StringBuilder()
      val resultList = new ListBuffer[Any]()
      val m_date = fmt.parse(x_arr(0))
      val cal = Calendar.getInstance(tz)
      cal.setTime(m_date)
      //
      // key
      //
      key_builder.append(x_arr(1)) // Station_Id
      key_builder.append(",").append(cal.get(Calendar.YEAR)) // Year
      key_builder.append(",").append(cal.get(Calendar.DAY_OF_YEAR)) // Day of Year
      //
      // data
      //
      resultList += m_date.getTime() // Epoch Time
      resultList += x_arr(7).toDouble
      resultList += x_arr(8).toDouble
      resultList += x_arr(9).toDouble
      resultList += x_arr(10).toDouble
      resultList += x_arr(11).toDouble
      //
      (key_builder.toString(), resultList)
    } else {
      ("", new ListBuffer())
    }
  }

  private def m_map_vector(tuple: Tuple2[String, Iterable[ListBuffer[Any]]]): Vector = {
    val key = tuple._1
    if (key.trim().length() > 0) {
      val values = tuple._2
      //
      val m_ordering = Ordering.by { x: ListBuffer[Any] => x(0).asInstanceOf[Long] }
      val set = TreeSet.empty(m_ordering)
      values.foreach(list => set.add(list))
      //
      val doubles_list = new ListBuffer[Double]()
      set.foreach { list => doubles_list += list(1).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(2).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(3).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(4).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(5).asInstanceOf[Double] }
      Vectors.dense(doubles_list.toArray)
    } else {
      Vectors.zeros(0)
    }
  }

  /**
   * Write the specified vectors into CSV file specified by filename.
   *
   * @param filename file to write to
   * @param m_list TraversableOnce[Vector]
   */
  def write_vectors(filename: String, m_list: TraversableOnce[Vector]) = {
    // written out as one row
    val csv_writer: CSVWriter = new CSVWriter(new FileWriter(new File(filename)))
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

  def write_matrix(filename: String, m_matrix: Matrix) = {
    // written out as column major matrix
    csvwrite(new File(filename), new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray))
  }
}