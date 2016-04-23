package org.ucsd.dse.capstone.traffic.deprecated

import scala.collection.Map
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Executes KMeans
 *
 * @author dyerke
 */
class KMeansExecutor(list_vectors: List[Vector], pivot_df: DataFrame, colEnum: PivotColumn, output_parameter: OutputParameter) extends Executor[Unit] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): Unit = {
    val working_vectors: List[Vector] = list_vectors.map { v =>
      val d_arr: Array[Double] = v.toArray
      val new_arr: Array[Double] = d_arr.slice(IOUtils.keyFldCnt, d_arr.length)
      Vectors.dense(new_arr)
    }
    //
    // execute KMeans and obtain labels for transformed vectors
    //
    println("execute KMeans and obtain labels for transformed vectors")
    val rdd: RDD[Vector] = sc.parallelize(working_vectors).cache()
    val num_clusters = 7
    val max_iter = 10
    val kmeans_model: KMeansModel = KMeans.train(rdd, num_clusters, max_iter)
    //
    val labels_list: List[(String, Int)] = list_vectors.map { v =>
      val d_arr: Array[Double] = v.toArray
      val id = d_arr.slice(0, IOUtils.keyFldCnt).map(_.toInt).map(_.toString).mkString(",")
      val vec: Vector = Vectors.dense(d_arr.slice(IOUtils.keyFldCnt, d_arr.length))
      (id, kmeans_model.predict(vec))
    }
    val rdd_key_labels: RDD[(String, Int)] = sc.parallelize(labels_list)
    //
    // join labels with original transformed space
    //
    println("join labels with original transformed space")
    val org_dim_rdd: RDD[(Array[Int], Vector)] = IOUtils.toVectorRDD_withKeys(pivot_df, colEnum)
    val rdd_orig_dim_key_labels: RDD[(String, Vector)] = org_dim_rdd.map { entry =>
      val id_arr: Array[Int] = entry._1
      val vec: Vector = entry._2
      //
      val id = id_arr.map(_.toString).mkString(",")
      (id, vec)
    }
    val labeled_pair: PairRDDFunctions[String, Int] = RDD.rddToPairRDDFunctions(rdd_key_labels)
    val joined: RDD[(String, (Int, Vector))] = labeled_pair.join(rdd_orig_dim_key_labels)
    if (joined.count() == 0) {
      throw new IllegalStateException("Joined dataframe should not be empty")
    }
    //
    // obtain kmeans result in original input space
    //
    println("obtain kmeans result in original input space")
    val kmeans_result_rdd: RDD[(Int, Vector)] = joined.map(_._2).cache()
    //
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
    IOUtils.dump_to_output(target_list, TOTAL_FLOW, "cluster_fractions", output_parameter)
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
      IOUtils.dump_to_output(stat_target_list, TOTAL_FLOW, s"cluster_${k}_label_stats", output_parameter);
    }
  }
}
