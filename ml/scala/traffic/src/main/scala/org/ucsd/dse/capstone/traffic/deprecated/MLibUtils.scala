package org.ucsd.dse.capstone.traffic.deprecated

import java.util.Arrays

import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.{ Matrix => BM }
import breeze.linalg.{ Vector => BV }
import breeze.linalg.{ svd => brzSvd }

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

    //
    // compute the Covariance matrix of the using RowMatrix.computeCovariance().
    // Underlying implementation uses Spark RDD treeAggregate function, allowing
    // for the distributed computation of the Covariance Matrix of the large dataset
    //
    val Cov: BDM[Double] = toBreeze(row_matrix.computeCovariance())

    //
    // Execute Singular Value Decomposition on the Covariance Matrix using Breeze
    //
    val brzSvd.SVD(u: BDM[Double], s: BDV[Double], _) = brzSvd(Cov)

    val eigenSum = s.data.sum
    val explainedVariance = s.data.map(_ / eigenSum)

    if (k == n) {
      //
      // return Eigenvectors and Eigenvalues
      //
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

  def toBreeze(m_vector: Vector): BDV[Double] = {
    new BDV[Double](m_vector.toArray)
  }

  def fromBreeze(v: BDV[Double]): Vector = {
    if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
      new DenseVector(v.data)
    } else {
      new DenseVector(v.toArray) // Can't use underlying array directly, so make a new one
    }
  }

  def fromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        new DenseMatrix(dm.rows, dm.cols, dm.toArray, dm.isTranspose)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }
}