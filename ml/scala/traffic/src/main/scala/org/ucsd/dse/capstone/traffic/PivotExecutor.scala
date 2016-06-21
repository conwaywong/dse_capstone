package org.ucsd.dse.capstone.traffic

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.TreeSet

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

/**
 * Class that pivots an RDD[String] consisting of rows of individual traffic readings to
 * columns of traffic readings. The resulting RDD[Row] consists of:
 *
 * 288 total flow 5m readings as columns
 * 288 occupancy 5m readings as columns
 * 288 speed 5m readings as columns
 *
 * Each row represents the readings for a traffic station in a given day.
 *
 * @author dyerke
 */
class PivotExecutor(files: List[String], output_path: String, s3out: Boolean = true) extends Executor[Unit] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*) = {
    // execute pivot
    val broadcast_expected_column_count = sc.broadcast((288 * 3) + 5)
    val broadcast_empty = sc.broadcast(("", new ListBuffer[Any]()))
    val broadcast_empty_row = sc.broadcast(Row.fromSeq(List()))
    //
    val m_string_rdd: RDD[String] = sc.textFile(files.mkString(","))
    //
    // execute pivot
    //
    val t_rdd: RDD[(String, ListBuffer[Any])] = m_string_rdd.map { line: String =>
      val EMPTY: (String, ListBuffer[Any]) = broadcast_empty.value
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
        key_builder.append(",").append(x_arr(2)) // District Id
        key_builder.append(",").append(cal.get(Calendar.YEAR)) // Year
        key_builder.append(",").append(cal.get(Calendar.DAY_OF_YEAR)) // Day of Year
        key_builder.append(",").append(cal.get(Calendar.DAY_OF_WEEK)) // Day of Week
        //
        // data
        //

        resultList += m_date.getTime() // Epoch Time
        try {
          resultList += x_arr(9).toDouble // Total Flow
          resultList += x_arr(10).toDouble // Avg. Occupancy
          resultList += x_arr(11).toDouble // Avg. Speed
        } catch {
          case e: Exception => EMPTY
        }
        //
        (key_builder.toString(), resultList)
      } else {
        EMPTY
      }
    }
    val pair_rdd: PairRDDFunctions[String, ListBuffer[Any]] = RDD.rddToPairRDDFunctions(t_rdd)
    val m_result_rdd: RDD[(String, Iterable[ListBuffer[Any]])] = pair_rdd.groupByKey()
    val row_rdd: RDD[Row] = m_result_rdd.map {
      tuple: Tuple2[String, Iterable[ListBuffer[Any]]] =>
        val EMPTY_ROW = broadcast_empty_row.value
        val expected_column_count = broadcast_expected_column_count.value
        val key = tuple._1
        if (key.trim().length() > 0) {
          try {
            val values = tuple._2
            //
            val m_ordering = Ordering.by { x: ListBuffer[Any] => x(0).asInstanceOf[Long] }
            val set = TreeSet.empty(m_ordering)
            values.foreach(list => set.add(list))
            //
            val row_contents = new ListBuffer[Any]()
            row_contents.appendAll(key.split(",")) // ID fields
            set.foreach { list => row_contents += list(1).asInstanceOf[Double] } // Total Flow
            set.foreach { list => row_contents += list(2).asInstanceOf[Double] } // Avg. Occupancy
            set.foreach { list => row_contents += list(3).asInstanceOf[Double] } // Avg. Speed
            val n = row_contents.size
            if (n < expected_column_count) {
              println("Expecting " + expected_column_count + " but obtained " + n + " for key " + key)
              EMPTY_ROW
            } else {
              Row.fromSeq(row_contents)
            }
          } catch {
            case e: Exception => EMPTY_ROW
          }
        } else {
          EMPTY_ROW
        }
    }.filter { row: Row => row.size > 0 }
    if (s3out)
      row_rdd.saveAsTextFile(output_path, classOf[BZip2Codec])
    else
      row_rdd.saveAsTextFile(output_path)
  }
}