package org.ucsd.dse.capstone.traffic

import org.apache.commons.io.FilenameUtils
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector

import com.amazonaws.services.s3.AmazonS3

sealed trait PivotColumn
case object TOTAL_FLOW extends PivotColumn
case object SPEED extends PivotColumn
case object OCCUPANCY extends PivotColumn

/**
 * Class to hold the PCA result
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

/**
 * Class to hold the PCAResult for each observation (total flow, speed, occupancy)
 */
class PCAResults(total_flow: PCAResult, speed: PCAResult, occupancy: PCAResult) {
  val m_total_flow = total_flow
  val m_speed = speed
  val m_occupancy = occupancy
}

/**
 * Class defining parameter to PCA Transform
 */
class PCATransformParameter(column: PivotColumn, k: Int = 2, mean: DenseVector, eigenvectors: DenseMatrix, output_param: OutputParameter) {
  val m_column = column
  val m_k = k
  val m_mean = mean
  val m_eigenvectors = eigenvectors
  val m_output_param = output_param
}

/**
 * Class used when having S3 as the destination output.
 */
class S3Parameter(client: AmazonS3, bucket_name: String) {
  val m_client = client
  val m_bucket_name = bucket_name
}

/**
 * Class specifying the output directory and an output id to uniquely identify the output file.
 */
class OutputParameter(output_fid: String, output_dir: String) {
  val m_output_fid = output_fid
  val m_output_dir = FilenameUtils.normalizeNoEndSeparator(output_dir + "/").concat("/")
}