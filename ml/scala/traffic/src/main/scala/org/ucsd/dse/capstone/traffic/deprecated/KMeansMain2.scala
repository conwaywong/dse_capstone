package org.ucsd.dse.capstone.traffic.deprecated

import scala.collection.Map
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.SQLUserDefinedType

/**
 * Driver that executes KMeans and
 *
 * @author dyerke
 */
object KMeansMain2 {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    val sqlContext: SQLContext = new SQLContext(sc)
    //
    val path = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/parquet"
    val pivot_df: DataFrame = IOUtils.read_pivot_df2(sqlContext, path)
    val rdd_vector: RDD[Vector] = IOUtils.toVectorRDD(pivot_df, TOTAL_FLOW, false)
    //
    val num_clusters = 7
    val max_iter = 30
    //
    val kmeans_model: KMeansModel = KMeans.train(rdd_vector, num_clusters, max_iter)
    val centroids: Array[Vector] = kmeans_model.clusterCenters
    val broadcast_model = sc.broadcast(kmeans_model)
    val kmeans_result_rdd: RDD[(Int, Vector)] = rdd_vector.map { v =>
      val model: KMeansModel = broadcast_model.value
      val label: Int = model.predict(v)
      (label, v)
    }.cache()
    //
    // obtain cluster fraction
    //
    val pair_rdd: PairRDDFunctions[Int, Vector] = RDD.rddToPairRDDFunctions(kmeans_result_rdd)
    val map_count: Map[Int, Long] = pair_rdd.countByKey()
    var total: Double = 0.0
    map_count.foreach { entry =>
      val label = entry._1
      val count = entry._2
      //
      println(s"Found $count for label $label")
      total += count.toDouble
    }
    println(s"Total=$total")
    val target_list: ListBuffer[Vector] = new ListBuffer[Vector]()
    map_count.foreach { entry =>
      val label = entry._1.toDouble
      val count = entry._2.toDouble
      //
      val fraction = count / total
      println(s"Fraction for label $label=$fraction")
      val new_row: Array[Double] = Array[Double](label, fraction)
      target_list += Vectors.dense(new_row)
    }
    IOUtils.write_vectors("/var/tmp/cluster_fractions.csv", target_list)
    //
    // obtain mean+std for each label
    //
    val broadcast_empty = sc.broadcast(Vectors.zeros(0))
    for (k <- 0 to num_clusters - 1) {
      println(s"Processing cluster label $k")
      //
      val broadcast_k = sc.broadcast(k)
      val working_rdd: RDD[Vector] = kmeans_result_rdd.map { entry =>
        val current_k = broadcast_k.value
        //
        val label = entry._1
        val current_vec = entry._2
        //
        if (label == current_k) current_vec else broadcast_empty.value
      }.filter { v => v.size > 0 }
      //
      val summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(working_rdd)
      val stat_target_list: ListBuffer[Vector] = new ListBuffer[Vector]()
      stat_target_list += summary_stats.mean
      stat_target_list += summary_stats.variance
      val filename = s"/var/tmp/cluster_${k}_label_stats.csv"
      IOUtils.write_vectors(filename, stat_target_list)
    }
  }
}
