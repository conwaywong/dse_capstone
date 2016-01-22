package org.ucsd.dse.capstone.traffic

import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.util.Arrays

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
   * Creates and persists a new RDD[String] created from specified file
   *
   * @param the SparkContext
   * @param filename filename or directory path to data
   * @param partition_count the number of partitions to divide the dataset into
   * @return RDD[String]
   */
  def new_rdd(sc: SparkContext, files: List[String], partition_count: Int = 4): RDD[String] = {
    val m_rdd: RDD[String] = sc.textFile(files.mkString(","), partition_count)
    m_rdd.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Pivots traffic data using specified PivotHandler
   *
   * @param m_string_rdd the traffic data as an RDD[String]
   * @param m_pivot_handler the pivot handler
   * @return RDD[Vector]
   */
  def pivot(m_string_rdd: RDD[String], m_pivot_handler: PivotHandler): RDD[Vector] = {
    m_pivot_handler.pivot(m_string_rdd)
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