package org.ucsd.dse.capstone.traffic

import java.util.Arrays

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * Driver that executes KMeans and
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
//    val paths: List[String] = List[String]("pivot_2008", "pivot_2009", "pivot_2010", "pivot_2011", "pivot_2013", "pivot_2014", "pivot_2015")
    val paths: List[String] = List[String]("pivot_2011", "pivot_2013", "pivot_2014", "pivot_2015")

    for (p <- paths) {
      val path = s"/home/dyerke/Documents/DSE/capstone_project/traffic/data/total_flow_transformed.${p}_grouping_pca_transform_tmp.csv"
      val centroid_output_path = s"/var/tmp/${p}_centroids.csv"
      val label_output_path = s"/var/tmp/${p}_labels.csv"
      execute_kmeans(sc, path, centroid_output_path, label_output_path)
    }

  }

  private def execute_kmeans(sc: SparkContext, path: String, centroid_output_path: String, label_output_path: String): Unit = {
    val list_vectors: List[Vector] = IOUtils.read_vectors(path)
    val rdd: RDD[Vector] = sc.parallelize(list_vectors).cache()

    val num_clusters = 10
    val max_iter = 300
    val error: Array[Double] = new Array(num_clusters)
    //
    for (k <- 0 to num_clusters - 1) {
      val num_clusters = k + 1
      println(s"Executing KMeans using $num_clusters clusters")

      val kmeans_model: KMeansModel = KMeans.train(rdd, num_clusters, max_iter)
      error(k) = kmeans_model.computeCost(rdd)
    }
    //
    val error_string = Arrays.toString(error)
    println(s"Determining optimal cluster count for error $error_string")
    val opt_cluster_count = get_optimal_cluster_count(error, threshold = 200.0)
    //
    println(s"Executing kmeans using optimal cluster count $opt_cluster_count")
    val kmeans_model: KMeansModel = KMeans.train(rdd, opt_cluster_count, max_iter)
    //
    val centroids: Array[Vector] = kmeans_model.clusterCenters
    IOUtils.write_vectors(centroid_output_path, centroids)
    //
    val sample_size: Int = 100000
    val seed: Long = 47
    val sample_arr: Array[Vector] = rdd.takeSample(false, sample_size, seed)
    val output_centroid_result: ListBuffer[Vector] = new ListBuffer[Vector]
    for (v <- sample_arr) {
      val label: Double = kmeans_model.predict(v).toDouble
      output_centroid_result += Vectors.dense(label, v.toArray: _*)
    }
    IOUtils.write_vectors(label_output_path, output_centroid_result)
  }

  private def get_optimal_cluster_count(error: Array[Double], power: Double = 13, threshold: Double = 10.0): Int = {
    // calculate optimal cluster count
    val new_error = error.map { d =>
      val t = Math.pow(10.0, power)
      d / t
    }
    // calculate error diff
    val new_error_diff: ListBuffer[Double] = new ListBuffer[Double]()
    for (i <- 1 to error.length - 1) {
      val cur = new_error(i)
      val prev = new_error(i - 1)
      new_error_diff += Math.abs((cur - prev))
    }

    var opt_cluster_count = -1
    breakable {
      for (i <- 0 to new_error_diff.length - 1) {
        val e = new_error_diff(i)
        if (e < threshold) {
          opt_cluster_count = i + 1
          break
        }
      }
    }
    if (opt_cluster_count == -1) {
      throw new IllegalStateException(s"Unable to determine optimal cluster count")
    }
    opt_cluster_count
  }
}
