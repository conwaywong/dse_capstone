package org.ucsd.dse.capstone.traffic

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.GroupedData
import org.apache.spark.sql.SQLContext

/**
 * Executes PCA, PCA transform, and aggregates eigenvector coefficients
 *
 * @author dyerke
 */
class StationPCATranformGroupingExecutor(pivot_df: DataFrame, grouping_output_parameter: OutputParameter, k: Int) extends Executor[PCATransformGroupResults] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): PCATransformGroupResults = {
    do_execute(sc, sql_context, pivot_df, grouping_output_parameter)
  }

  private def do_execute(sc: SparkContext, sql_context: SQLContext, pivot_df: DataFrame, grouping_output_parameter: OutputParameter): PCATransformGroupResults = {
    val s3_param = grouping_output_parameter.m_s3_param
    val fid = grouping_output_parameter.m_output_fid
    //
    // execute PCA
    //
    val pca_output_parameter = if (s3_param != null) new OutputParameter(fid + "_pca_tmp", "output", s3_param) else new OutputParameter(fid + "_pca_tmp", "/var/tmp/output")
    val executor: Executor[PCAResults] = new PCAExecutor(pivot_df, pca_output_parameter)
    val pca_results: PCAResults = executor.execute(sc, sql_context)
    //
    // execute PCA transform
    //
    val rdd_results: List[RDD[(Array[Int], Vector)]] = IOUtils.get_total_flow_rdd_partitions(sc, pivot_df)
    val arr_results: List[PCAResult] = List(pca_results.m_total_flow_weekday, pca_results.m_total_flow_weekend)
    val token_names: List[String] = List("weekday", "weekend")
    val results: ListBuffer[Array[Vector]] = new ListBuffer[Array[Vector]]()
    val column: PivotColumn = TOTAL_FLOW
    for (i <- 0 to 2 - 1) {
      val rdd = rdd_results(i)
      val working_pca_result = arr_results(1)
      val token_name = token_names(i)
      //
      var s = List(fid, token_name, "pca_transform_tmp").mkString("_")
      val pca_trans_output_parameter = if (s3_param != null) new OutputParameter(s, "output", s3_param) else new OutputParameter(s, "/var/tmp/output")
      val trans_parameter: PCATransformParameter = new PCATransformParameter(column, working_pca_result, pca_trans_output_parameter, k)

      println("Executing PCATransformExecutor2")
      val trans_executor: Executor[Array[Vector]] = new PCATransformExecutor2(rdd, trans_parameter)
      val trans_result: Array[Vector] = trans_executor.execute(sc, sql_context)
      //
      // execute grouping and obtain mean
      //
      val trans_df: DataFrame = IOUtils.toTransformedDf(sql_context, trans_result, k)
      val groupby_columns: List[Column] = List[Column](new Column("station_id"), new Column("direction"))
      val groups: GroupedData = trans_df.groupBy(groupby_columns: _*)
      val colnames: ListBuffer[String] = new ListBuffer[String]()
      for (i <- 0 to k - 1) {
        colnames += ("V" + i).toString()
      }
      val result_df: DataFrame = groups.mean(colnames: _*)
      //
      val result_arr: Array[Vector] = result_df.rdd.map(row => Vectors.dense(row.toSeq.toArray.map { x => x.asInstanceOf[Double] })).collect()
      IOUtils.dump_vec_to_output(result_arr, token_name + "_transformed", grouping_output_parameter, column)
      //
      results += result_arr
    }
    new PCATransformGroupResults(results(0), results(1))
  }
}