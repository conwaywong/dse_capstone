package org.ucsd.dse.capstone.traffic.deprecated

import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.SQLContext

/**
 * Executes PCA, PCA transform, and aggregates eigenvector coefficients
 *
 * @author dyerke
 */
class StationPCATranformGroupingExecutor(pivot_df: DataFrame, column: PivotColumn, grouping_output_parameter: OutputParameter, k: Int) extends Executor[Tuple2[Array[Vector], String]] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): Tuple2[Array[Vector], String] = {
    do_execute(sc, sql_context, pivot_df, column, grouping_output_parameter)
  }

  private def do_execute(sc: SparkContext, sql_context: SQLContext, pivot_df: DataFrame, column: PivotColumn, grouping_output_parameter: OutputParameter): Tuple2[Array[Vector], String] = {
    val s3_param = grouping_output_parameter.m_s3_param
    val fid = grouping_output_parameter.m_output_fid
    //
    // execute PCA
    //
    val pca_output_parameter = if (s3_param != null) new OutputParameter(fid + "_pca_tmp", "output", s3_param) else new OutputParameter(fid + "_pca_tmp", "/var/tmp/output")
    val executor: Executor[PCAResults] = new PCAExecutor(pivot_df, pca_output_parameter, m_column_prefixes = List[PivotColumn](column))
    val pca_results: PCAResults = executor.execute(sc, sql_context)
    //
    // execute PCA transform
    //
    var working_pca_result: PCAResult = null
    column match {
      case TOTAL_FLOW => working_pca_result = pca_results.m_total_flow
      case OCCUPANCY  => working_pca_result = pca_results.m_occupancy
      case SPEED      => working_pca_result = pca_results.m_speed
      case _          => throw new IllegalStateException("Unknown column type: " + column)
    }
    val pca_trans_output_parameter = if (s3_param != null) new OutputParameter(fid + "_pca_transform_tmp", "output", s3_param) else new OutputParameter(fid + "_pca_transform_tmp", "/var/tmp/output")
    val trans_parameter: PCATransformParameter = new PCATransformParameter(column, working_pca_result, pca_trans_output_parameter, k)

    println("Executing PCATransformExecutor2")
    val trans_executor: Executor[Tuple2[Array[Vector], String]] = new PCATransformExecutor2(pivot_df, trans_parameter)
    val trans_result: Tuple2[Array[Vector], String] = trans_executor.execute(sc, sql_context)
    //
    // execute grouping and obtain mean
    //
    val filename_prefix = IOUtils.get_col_prefix(column)
    val output_dir = grouping_output_parameter.m_output_dir
    val grouping_output_filename = output_dir + filename_prefix + "_" + fid + ".csv"

    val trans_df: DataFrame = IOUtils.toTransformedDf(sql_context, trans_result._1, k)
    val groupby_columns: List[Column] = List[Column](new Column("station_id"), new Column("direction"))
    val groups: GroupedData = trans_df.groupBy(groupby_columns: _*)
    val colnames: ListBuffer[String] = new ListBuffer[String]()
    for (i <- 0 to k - 1) {
      colnames += ("V" + i).toString()
    }
    val result_df: DataFrame = groups.mean(colnames: _*)
    //
    val result_arr: Array[Vector] = result_df.rdd.map(row => Vectors.dense(row.toSeq.toArray.map { x => x.asInstanceOf[Double] })).collect()
    val result_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
    IOUtils.write_vectors(grouping_output_filename, result_arr, filename => {
      new BufferedWriter(new OutputStreamWriter(result_stream))
    })
    if (s3_param != null) {
      IOUtils.process_stream(s3_param.m_client, s3_param.m_bucket_name, output_dir, grouping_output_filename, result_stream)
    } else {
      IOUtils.process_stream(output_dir, grouping_output_filename, result_stream)
    }
    (result_arr, grouping_output_filename)
  }
}