package org.ucsd.dse.capstone.traffic

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.TimeZone

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.TreeSet

import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Private
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * @author dyerke
 */
object PivotMain extends Logging {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  def do_execute(sc: SparkContext) = {
    // execute pivot
    val broadcast_logger = sc.broadcast(log)
    val broadcast_expected_column_count = sc.broadcast((288 * 3) + 5)
    //
    val files: List[String] = List("/home/dyerke/Documents/DSE/capstone_project/traffic/data/d11_text_station_5min_2015_01_01_mod.txt")
    val m_string_rdd: RDD[String] = sc.textFile(files.mkString(","))
    //
    // execute pivot
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
        key_builder.append(",").append(x_arr(2)) // District Id
        key_builder.append(",").append(cal.get(Calendar.YEAR)) // Year
        key_builder.append(",").append(cal.get(Calendar.DAY_OF_YEAR)) // Day of Year
        key_builder.append(",").append(cal.get(Calendar.DAY_OF_WEEK)) // Day of Week
        //
        // data
        //
        resultList += m_date.getTime() // Epoch Time
        resultList += x_arr(9).toDouble // Total Flow
        resultList += x_arr(10).toDouble // Avg. Occupancy
        resultList += x_arr(11).toDouble // Avg. Speed
        //
        (key_builder.toString(), resultList)
      } else {
        ("", new ListBuffer())
      }
    }
    val pair_rdd: PairRDDFunctions[String, ListBuffer[Any]] = RDD.rddToPairRDDFunctions(t_rdd)
    val m_result_rdd: RDD[(String, Iterable[ListBuffer[Any]])] = pair_rdd.groupByKey()
    val row_rdd: RDD[Row] = m_result_rdd.map {
      tuple: Tuple2[String, Iterable[ListBuffer[Any]]] =>
        val expected_column_count = broadcast_expected_column_count.value
        val key = tuple._1
        if (key.trim().length() > 0) {
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
            log.error("Expecting " + expected_column_count + " but obtained " + n + " for key " + key)
            Row.fromSeq(List())
          } else {
            if (log.isInfoEnabled()) {
              log.info("Found " + n + " readings, including key " + key + " into dataset")
            }
            Row.fromSeq(row_contents)
          }
        } else {
          Row.fromSeq(List())
        }
    }.filter { row: Row => row.size > 0 }
    //
    // create schema and persist parquet file
    //
    //    val m_struct_fields = new ListBuffer[StructField]()
    //    m_struct_fields.append(new StructField("station_id", StringType))
    //    m_struct_fields.append(new StructField("district_id", StringType))
    //    m_struct_fields.append(new StructField("year", StringType))
    //    m_struct_fields.append(new StructField("day_of_year", StringType))
    //    m_struct_fields.append(new StructField("day_of_week", StringType))
    //    //
    //    val m_obversation_prefixes = List("total_flow_", "occupancy_", "speed_")
    //    m_obversation_prefixes.foreach { prefix =>
    //      for (i <- 0 to 287) {
    //        var min = (5 * (i + 1)).toString()
    //        val b = new StringBuilder()
    //        for (j <- 0 to (4 - min.size - 1)) {
    //          b.append("0")
    //        }
    //        val column_name = prefix + b.append(min).append("M").toString
    //        m_struct_fields.append(new StructField(column_name, DoubleType))
    //      }
    //    }
    //    val schema = StructType(m_struct_fields.toArray)
    //    //
    //    val sqlContext = new SQLContext(sc)
    //    import sqlContext.implicits._
    //    //
    //    val df = sqlContext.createDataFrame(row_rdd, schema)
    //    df.write.parquet("/tmp/test_output/traffic.parquet")
    //
    //
    // persist
    //
    val output_dir = "/tmp/test_output2"
    row_rdd.saveAsTextFile(output_dir, classOf[BZip2Codec])
  }
}