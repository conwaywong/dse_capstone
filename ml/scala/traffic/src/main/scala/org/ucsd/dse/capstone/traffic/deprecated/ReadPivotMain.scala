package org.ucsd.dse.capstone.traffic.deprecated

import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Private

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column

// Import Spark SQL data types
import org.apache.spark.sql.types._

/**
 * Driver that deserializes RDD[Row] from compressed text files into a Spark SQL DataFrame.
 * 
 * @cwong
 */
object ReadPivotMain extends Logging {

  def main(args: Array[String]) {
    val template: SparkTemplate = new DefaultSparkTemplate()
    //
    template.execute { sc => do_execute(sc) }
  }

  // CONSTANTS
  val input = "/var/tmp/test_output2"

  def do_execute(sc: SparkContext) = {

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Create Schema
    var schema = new org.apache.spark.sql.types.StructType()

    schema = schema.add(new StructField("station_id", IntegerType))
    schema = schema.add(new StructField("district_id", IntegerType))
    schema = schema.add(new StructField("year", IntegerType))
    schema = schema.add(new StructField("day_of_year", IntegerType))
    schema = schema.add(new StructField("day_of_week", IntegerType))

    val m_obversation_prefixes = List("total_flow", "occupancy", "speed")

    val m_observation_times = (0 to 287).map((_ * 5)).map(s => f"${s / 60}%02d${s % 60}%02dM")
    m_obversation_prefixes.foreach { prefix =>
      m_observation_times.foreach { time_str =>
        schema = schema.add(StructField(s"${prefix}_${time_str}", DoubleType))
      }
    }

    val m_string_rdd: RDD[String] = sc.textFile(input, 4)

    val rdd = m_string_rdd.map(s => s.substring(1, s.length - 1)).map(s => s.split(",")).map(p => Row.fromSeq(List.concat(p.slice(0, 5).map(_.toInt), p.slice(5, p.length).map(_.toDouble))))

    val pivot_df = sqlContext.createDataFrame(rdd, schema)

    
    // ===============  Below is DataFrame query examples
    pivot_df.registerTempTable("pivot_all")

    val results = sqlContext.sql("SELECT DISTINCT station_id FROM pivot_all")
    results.show

    // Build total_flow column names
    val total_flow_columns: Seq[Column] = for (i <- m_observation_times) yield new Column(s"total_flow_${i}")

    // Create DataFrame which only contains the total_flow columns
    // Note the casting of the Seq[Column] into varargs
    val total_flow_df = pivot_df.select(total_flow_columns: _*)

    // Get the RDD representation of the dataframe
    val total_flow_rdd = total_flow_df.rdd
  }
  
}