package org.ucsd.dse.capstone.traffic

import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.io.Writer

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import au.com.bytecode.opencsv.CSVWriter
import breeze.io.{ CSVWriter => BCSV }
import breeze.linalg.{ DenseMatrix => BDM }

object PivotColumnPrefixes 
{
  val TOTAL_FLOW = 1
  val SPEED = 2
  val OCCUPANCY = 3
}

object IOUtils
{
    var keyFldCnt:Int = 0
    var FldCnt:Int = 288
    
    private def get_schema(): (StructType, IndexedSeq[String]) =
    {
        // Create Schema
        var schema = new StructType()

        schema = schema.add(new StructField("station_id", IntegerType))
        schema = schema.add(new StructField("district_id", IntegerType))
        schema = schema.add(new StructField("year", IntegerType))
        schema = schema.add(new StructField("day_of_year", IntegerType))
        schema = schema.add(new StructField("day_of_week", IntegerType))

        keyFldCnt = schema.fields.length

        val m_obversation_prefixes = List("total_flow", "occupancy", "speed")

        val m_observation_times: IndexedSeq[String] = (0 to (FldCnt-1)).map((_ * 5)).map(s => f"${s / 60}%02d${s % 60}%02dM")
        m_obversation_prefixes.foreach { prefix =>
            m_observation_times.foreach { time_str =>
                schema = schema.add(StructField(s"${prefix}_${time_str}", DoubleType))
            }
        }
        (schema, m_observation_times)
    }
    
    private def get_col_prefix(col_enum:Int) : String = 
    {
       col_enum match {
            case PivotColumnPrefixes.TOTAL_FLOW => "total_flow_"
            case PivotColumnPrefixes.SPEED      => "speed_"
            case PivotColumnPrefixes.OCCUPANCY  => "occupancy_"
        }
    }
    
    def read_pivot_df(sc: SparkContext, sqlContext: SQLContext, path: String, table_name: String = "pivot_all"): DataFrame = 
    {
        // Create Schema
        val (schema, m_observation_times) = get_schema()
        // create DataFrame
        val m_string_rdd: RDD[String] = sc.textFile(path)
        val m_row_rdd: RDD[Row] = m_string_rdd.map { s =>
            var s_csv: String = null
            try
            {
                s_csv = s.substring(1, s.length - 1) // Trim off leading and trailing '[', ']' characters
                val s_arr: Array[String] = s_csv.split(",")
                val key_arr: Array[Int] = s_arr.slice(0, keyFldCnt).map(_.toInt)
                val values_arr: Array[Double] = s_arr.slice(keyFldCnt, s_arr.length).map(_.toDouble)
                Row.fromSeq(List.concat(key_arr, values_arr))
            }
            catch
            {
                case e: Exception => throw new IllegalStateException("Error parsing " + s_csv, e)
            }
        }
        val pivot_df: DataFrame = sqlContext.createDataFrame(m_row_rdd, schema)
        // Register the DataFrames as a table.
        pivot_df.registerTempTable(table_name)
        pivot_df
    }
    
    def toVectorRDD_withKeys(pivotDF: DataFrame, colEnum: Int) : RDD[(Array[Int], Vector)] =
    {
        val data_rdd:RDD[Vector] = toVectorRDD(pivotDF, colEnum, true)
        data_rdd.map { r => (r.toArray.slice(0, keyFldCnt).map(_.toInt), Vectors.dense(r.toArray.slice(keyFldCnt, FldCnt+keyFldCnt))) }
    }

    def toVectorRDD(pivotDF: DataFrame, colEnum: Int, includeKeys:Boolean = false): RDD[Vector] =
    {
        val col_prefix = get_col_prefix(colEnum)
        var nCols = FldCnt

        val (schema, obos_times) = get_schema()

        // Build total_flow column names
        val columns: ListBuffer[Column] = new ListBuffer[Column]()
        if(includeKeys)
        {
            Seq(schema.fieldNames.slice(0, keyFldCnt).foreach { f => columns += new Column(f) })
            nCols += keyFldCnt
        }
        for (i <- obos_times)
            columns += new Column(s"${col_prefix}${i}")
        require(columns.length == nCols, "obos_times.length= " + + obos_times.length + "; columns.length= " + + columns.length + "; Columns is not " + nCols)

        // create DataFrame which only contains the desired columns
        // Note the casting of the Seq[Column] into var args
        // convert to RDD[Vector]
        val rddRows = pivotDF.select(columns: _*).rdd
        rddRows.map(x => Vectors.dense(x.toSeq.map(_.asInstanceOf[Double]).toArray))
    }

    /* Wrapper to write to file */
    def write_vectors(filename: String, m_list: TraversableOnce[Vector]): Unit =
    {
        write_vectors(filename, m_list, filename => { new FileWriter(new File(filename)) })
    }

    def write_vectors(filename: String, m_list: TraversableOnce[Vector], new_writer: (String) => Writer): Unit =
    {
        val csv_writer: CSVWriter = new CSVWriter(new_writer(filename))
        try
        {
            try
            {
                m_list.foreach { a_vec: Vector =>
                    val m_vec = a_vec.toArray
                    val m_string_vec: Array[String] = new Array[String](m_vec.length)
                    for (i <- 0 to m_vec.length - 1) {
                        m_string_vec(i) = m_vec(i).toString()
                    }
                    csv_writer.writeNext(m_string_vec)
                }
            }
            finally
            {
                csv_writer.close()
            }
        }
        catch
        {
            case e: IOException => throw new IllegalStateException(e)
        }
    }

    /* Wrapper to write to file */
    def write_matrix(filename: String, m_matrix: Matrix): Unit =
    {
        write_matrix(filename, m_matrix, filename => { new FileWriter(new File(filename)) })
    }

    def write_matrix(filename: String, m_matrix: Matrix, new_writer: (String) => Writer): Unit =
    {
        // written out as column major matrix
        val mat: BDM[Double] = new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray)
        BCSV.write(new_writer(filename), IndexedSeq.tabulate(mat.rows, mat.cols)(mat(_, _).toString))
    }
}