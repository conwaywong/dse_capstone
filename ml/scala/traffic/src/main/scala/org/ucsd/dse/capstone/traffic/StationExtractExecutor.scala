package org.ucsd.dse.capstone.traffic

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
 * Obtain rows for specified station ids
 *
 * @author dyerke
 */
class StationExtractExecutor(m_pair_rdd: RDD[(Array[Int], Vector)], station_ids: Array[Int], output_parameter: OutputParameter) extends Executor[Unit] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): Unit = {
    do_execute(sc, m_pair_rdd, station_ids, output_parameter)
  }

  private def do_execute(sc: SparkContext, m_pair_rdd: RDD[(Array[Int], Vector)], station_ids: Array[Int], output_parameter: OutputParameter): Unit = {
    val broadcast_station_ids = sc.broadcast(station_ids)
    val broadcast_empty = sc.broadcast(Vectors.zeros(0))

    val vec_rdd: RDD[Vector] = m_pair_rdd.map { entry =>
      val id_arr: Array[Int] = entry._1
      val vec: Vector = entry._2
      //
      val l_station_ids: Array[Int] = broadcast_station_ids.value
      var a_result: Vector = null
      val sid= id_arr(0)
      println("sid= " + sid)
      if (l_station_ids.contains(sid)) {
        val d_arr = vec.toArray
        val result_list: ListBuffer[Double] = new ListBuffer[Double]()
        for (key <- id_arr) {
          result_list += key.toDouble
        }
        result_list.appendAll(d_arr)
        a_result = Vectors.dense(result_list.toArray)
      }
      if (a_result == null) {
        a_result = broadcast_empty.value
      }
      a_result
    }
    val vec_arr: Array[Vector] = vec_rdd.filter(_.size > 0).collect()
    IOUtils.dump_vec_to_output(vec_arr, "station_extract", output_parameter)
  }
}