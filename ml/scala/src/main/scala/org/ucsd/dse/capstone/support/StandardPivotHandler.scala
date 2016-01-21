package org.ucsd.dse.capstone.traffic.support

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.TreeSet

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD

/**
 * @author dyerke
 */

object Fields {
  val TotalFlow= 1
  val Occupancy= 2
  val Speed= 3
}

class StandardPivotHandler(sc: SparkContext, fields_enum: Int) extends PivotHandler {
  
  var m_sc = sc
  var m_fields_enum = fields_enum

  def pivot(m_string_rdd: RDD[String]): RDD[Vector] = {
    val broadcast_fields_enum = m_sc.broadcast(m_fields_enum)
    //
    val t_rdd: RDD[(String, ListBuffer[Any])] = m_string_rdd.map { line: String =>
      val x_arr = line.split(",")
      if (x_arr(5) == "ML") {
        //
        val tz = TimeZone.getTimeZone("UTC")
        val fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
        fmt.setTimeZone(tz)
        val key_builder = new StringBuilder()
        val resultList = new ListBuffer[Any]()
        val m_date = fmt.parse(x_arr(0))
        val cal = Calendar.getInstance(tz)
        cal.setTime(m_date)
        //
        // key
        //
        key_builder.append(x_arr(1)) // Station_Id
        key_builder.append(",").append(cal.get(Calendar.YEAR)) // Year
        key_builder.append(",").append(cal.get(Calendar.DAY_OF_YEAR)) // Day of Year
        //
        // data
        //
        resultList += m_date.getTime() // Epoch Time
        broadcast_fields_enum.value match {
          case Fields.TotalFlow => resultList += x_arr(9).toDouble // Total Flow
          case Fields.Occupancy => resultList += x_arr(10).toDouble // Avg. Occupancy
          case Fields.Speed     => resultList += x_arr(11).toDouble // Avg. Speed
        }
        //
        (key_builder.toString(), resultList)
      } else {
        ("", new ListBuffer())
      }
    }
    val pair_rdd: PairRDDFunctions[String, ListBuffer[Any]] = RDD.rddToPairRDDFunctions(t_rdd)
    val m_result_rdd: RDD[(String, Iterable[ListBuffer[Any]])] = pair_rdd.groupByKey()
    m_result_rdd.map {
      tuple: Tuple2[String, Iterable[ListBuffer[Any]]] =>
        val key = tuple._1
        if (key.trim().length() > 0) {
          val values = tuple._2
          //
          val m_ordering = Ordering.by { x: ListBuffer[Any] => x(0).asInstanceOf[Long] }
          val set = TreeSet.empty(m_ordering)
          values.foreach(list => set.add(list))
          //
          val doubles_list = new ListBuffer[Double]()
          set.foreach { list => doubles_list += list(1).asInstanceOf[Double] }
          Vectors.dense(doubles_list.toArray)
        } else {
          Vectors.zeros(0)
        }
    }.filter { v => v.size > 0 }
  }
}