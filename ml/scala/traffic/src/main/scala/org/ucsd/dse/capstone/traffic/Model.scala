package org.ucsd.dse.capstone.traffic

import org.apache.commons.io.FilenameUtils
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT

import com.amazonaws.services.s3.AmazonS3

sealed trait PivotColumn
case object TOTAL_FLOW extends PivotColumn

/**
 * Class to hold the PCA result
 */
class PCAResult(
    eigenvectors: Matrix,
    eigenvalues: Vector,
    meanvector: Vector) {
  val m_eig = eigenvectors
  val m_eig_values = eigenvalues
  val m_mean_vec = meanvector

  override def toString(): String = s"(m_eig=$m_eig; m_eig_values=$m_eig_values; m_mean_vec=$m_mean_vec)"
}

class RegressorResult(r2score: Double, feature_importance: List[(String, Double)]) {
  val m_r2score = r2score
  val m_feature_importance = feature_importance

  override def toString(): String = s"(m_r2score=$m_r2score; m_feature_importance=$m_feature_importance"
}

/**
 * Class to hold the PCAResult for each weekday total flow and weekend total flow
 */
class PCAResults(total_flow_weekday: PCAResult, total_flow_weekend: PCAResult) {
  val m_total_flow_weekday = total_flow_weekday
  val m_total_flow_weekend = total_flow_weekend

  override def toString(): String = s"(m_total_flow_weekday=$m_total_flow_weekday; m_total_flow_weekend=$m_total_flow_weekend)"
}

/**
 * Class to hold the PCAResult for each weekday total flow and weekend total flow
 */
class PCATransformGroupResults(trans_total_flow_weekday: Array[Vector], trans_total_flow_weekend: Array[Vector]) {
  val m_trans_total_flow_weekday = trans_total_flow_weekday
  val m_trans_total_flow_weekend = trans_total_flow_weekend

  override def toString(): String = s"(m_trans_total_flow_weekday=$m_trans_total_flow_weekday; m_trans_total_flow_weekend=$m_trans_total_flow_weekend)"
}

/**
 * Class defining parameter to PCA Transform
 */
class PCATransformParameter(column: PivotColumn, mean: Vector, eigenvectors: Matrix, output_param: OutputParameter, k: Int) {
  val m_column = column
  val m_mean = mean
  val m_eigenvectors = eigenvectors
  val m_output_param = output_param
  val m_k = k

  def this(column: PivotColumn, pca_result: PCAResult, output_param: OutputParameter, k: Int) {
    this(column, pca_result.m_mean_vec, pca_result.m_eig, output_param, k)
  }
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
class OutputParameter(output_fid: String, output_dir: String, s3_param: S3Parameter = null) {
  val m_output_fid = output_fid
  val m_output_dir = FilenameUtils.normalizeNoEndSeparator(output_dir + "/").concat("/")
  val m_s3_param = s3_param
}