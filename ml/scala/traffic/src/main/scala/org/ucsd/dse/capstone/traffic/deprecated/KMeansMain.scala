package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Driver that executes KMeans
 *
 * @author dyerke
 */
object KMeansMain {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
    //    val error: Array[Double] = List[Double](1.309800526251739E22, 2.69536391437906464E17, 8.5218442322906272E16, 2.132341126975352E16, 8.480174068368626E15, 6.165589548067214E15, 4.241745405510605E15, 2.4522670082592655E15, 1.808296553030303E15, 1.3827280313098578E15).toArray
    //    println(get_optimal_cluster_count(error, threshold = 200.0))
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val paths = List(
      ("/home/dyerke/Documents/DSE/capstone_project/traffic/data/total_flow_transformed.pivot_2008_grouping_pca_transform_tmp.csv", "/home/dyerke/Documents/DSE/capstone_project/traffic/data/parquet_2008"))
    for (p <- paths) {
      val path = p._1
      val parquet_path = p._2
      //
      val list_vectors: List[Vector] = IOUtils.read_vectors(path)
      val pivot_df: DataFrame = IOUtils.read_pivot_df2(sqlContext, parquet_path)
      val output_parameter: OutputParameter = new OutputParameter("test_kmeans", "/var/tmp")
      //
      val executor: Executor[Unit] = new KMeansExecutor(list_vectors, pivot_df, TOTAL_FLOW, output_parameter)
      executor.execute(sc, sqlContext)
    }
  }
}
