package org.ucsd.dse.capstone.traffic.deprecated

import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.OutputStream
import java.io.Reader
import java.io.Writer
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.SQLUserDefinedType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3Object
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import breeze.io.{ CSVReader => BCSVR }
import breeze.io.{ CSVWriter => BCSV }
import breeze.linalg.{ DenseMatrix => BDM }
import java.io.OutputStreamWriter
import java.io.BufferedWriter

/**
 * IO Utilities that deserializes RDD[Row] in compressed text format to RDD[Vector] for use in PCA
 *
 * @author cwong, dyerke, jgill
 */
object IOUtils {
  val OBSERVATION_COUNT: Int = 288
  val DIRECTION_INDEX_COUNT: Int = 4
  //
  val keyFldCnt: Int = 6

  private def get_key_schema(as_int: Boolean = true): StructType = {
    var schema = new StructType()
    if (as_int) {
      schema = schema.add(new StructField("station_id", IntegerType))
      schema = schema.add(new StructField("district_id", IntegerType))
      schema = schema.add(new StructField("year", IntegerType))
      schema = schema.add(new StructField("day_of_year", IntegerType))
      schema = schema.add(new StructField("day_of_week", IntegerType))
      schema = schema.add(new StructField("direction", IntegerType))
    } else {
      schema = schema.add(new StructField("station_id", DoubleType))
      schema = schema.add(new StructField("district_id", DoubleType))
      schema = schema.add(new StructField("year", DoubleType))
      schema = schema.add(new StructField("day_of_year", DoubleType))
      schema = schema.add(new StructField("day_of_week", DoubleType))
      schema = schema.add(new StructField("direction", DoubleType))
    }
    schema
  }

  private def get_schema(): (StructType, IndexedSeq[String]) = {
    // Create Schema
    var schema = get_key_schema()
    val m_obversation_prefixes = List("total_flow", "occupancy", "speed")

    val m_observation_times: IndexedSeq[String] = (0 to (OBSERVATION_COUNT - 1)).map((_ * 5)).map(s => f"${s / 60}%02d${s % 60}%02d")
    m_obversation_prefixes.foreach { prefix =>
      m_observation_times.foreach { time_str =>
        schema = schema.add(StructField(s"${prefix}_${time_str}", DoubleType))
      }
    }
    (schema, m_observation_times)
  }

  private def get_transformed_schema(k: Int): StructType = {
    // Create Schema
    var schema = get_key_schema(false)
    for (i <- 0 to k - 1) {
      schema = schema.add(StructField(s"V${i}", DoubleType))
    }
    schema
  }

  def get_col_prefix(col_enum: PivotColumn): String = {
    col_enum match {
      case TOTAL_FLOW => "total_flow_"
      case SPEED      => "speed_"
      case OCCUPANCY  => "occupancy_"
    }
  }

  def read_pivot_df(sc: SparkContext, sqlContext: SQLContext, path: String, table_name: String = "pivot_all"): DataFrame = {
    // Create Schema
    val (schema, m_observation_times) = get_schema()
    // create DataFrame
    val m_string_rdd: RDD[String] = sc.textFile(path)
    val m_row_rdd: RDD[Row] = m_string_rdd.map { s =>
      var s_csv: String = null
      try {
        s_csv = s.substring(1, s.length - 1) // Trim off leading and trailing '[', ']' characters
        val s_arr: Array[String] = s_csv.split(",")
        val key_arr: Array[Int] = s_arr.slice(0, keyFldCnt).map(_.toInt)
        val values_arr: Array[Double] = s_arr.slice(keyFldCnt, s_arr.length).map(_.toDouble)
        Row.fromSeq(List.concat(key_arr, values_arr))
      } catch {
        case e: Exception => throw new IllegalStateException("Error parsing " + s_csv, e)
      }
    }
    val pivot_df: DataFrame = sqlContext.createDataFrame(m_row_rdd, schema)
    // Register the DataFrames as a table.
    pivot_df.registerTempTable(table_name)
    pivot_df
  }

  def read_pivot_df2(sqlContext: SQLContext, path: String, table_name: String = "pivot_all"): DataFrame = {
    val df: DataFrame = sqlContext.read.parquet(path)
    val df2 = df.withColumn("directionTmp", df("direction").cast(DoubleType)).drop("direction").withColumnRenamed("directionTmp", "direction")
    //
    val one_hot_encoder: OneHotEncoder = new OneHotEncoder()
    one_hot_encoder.setInputCol("direction").setOutputCol("direction_index")
    val result_df = one_hot_encoder.transform(df2)

    //
    result_df.registerTempTable(table_name)
    result_df
  }

  def toTransformedDf(sqlContext: SQLContext, v: Array[Vector], k: Int): DataFrame = {
    val schema: StructType = get_transformed_schema(k)
    val rlist: java.util.List[Row] = new java.util.ArrayList[Row]()
    v.foreach { x =>
      rlist.add(Row.fromSeq(x.toArray))
    }
    sqlContext.createDataFrame(rlist, schema)
  }

  def toVectorRDD_withKeys(pivotDF: DataFrame, colEnum: PivotColumn): RDD[(Array[Int], Vector)] = {
    val data_rdd: RDD[Vector] = toVectorRDD(pivotDF, colEnum, true)
    val expected_column_count = OBSERVATION_COUNT + DIRECTION_INDEX_COUNT
    data_rdd.map { r => (r.toArray.slice(0, keyFldCnt).map(_.toInt), Vectors.dense(r.toArray.slice(keyFldCnt, expected_column_count + keyFldCnt))) }
  }

  def toVectorRDD(pivotDF: DataFrame, colEnum: PivotColumn, includeKeys: Boolean = false): RDD[Vector] = {
    val col_prefix = get_col_prefix(colEnum)
    var nCols = OBSERVATION_COUNT

    val (schema, obos_times) = get_schema()

    // Build column name projection
    val columns: ListBuffer[Column] = new ListBuffer[Column]()
    if (includeKeys) {
      Seq(schema.fieldNames.slice(0, keyFldCnt).foreach { f => columns += new Column(f) })
      nCols += keyFldCnt
    }
    for (i <- obos_times)
      columns += new Column(s"${col_prefix}${i}")

    val target_columns_size = nCols
    require(columns.length == target_columns_size, "obos_times.length= " + +obos_times.length + "; columns.length= " + +columns.length + "; Columns is not " + target_columns_size)

    // create DataFrame which only contains the desired columns
    // Note the casting of the Seq[Column] into var args
    // convert to RDD[Vector]
    val rddRows = pivotDF.select(columns: _*).rdd
    rddRows.map { row =>
      val target_list: ListBuffer[Double] = new ListBuffer[Double]()
      row.toSeq.foreach { x =>
        x match {
          case d: Double       => target_list += d
          case i: Int          => target_list += i.toDouble
          case v: SparseVector => target_list.appendAll(v.toArray)
          case _               => throw new IllegalStateException("Unknown type: " + x)
        }
      }
      Vectors.dense(target_list.toArray)
    }
  }

  /* Wrapper to write to file */
  def write_vectors(filename: String, m_list: TraversableOnce[Vector]): Unit = {
    write_vectors(filename, m_list, filename => { new FileWriter(new File(filename)) })
  }

  def write_vectors(filename: String, m_list: TraversableOnce[Vector], new_writer: (String) => Writer): Unit = {
    val csv_writer: CSVWriter = new CSVWriter(new_writer(filename))
    try {
      try {
        m_list.foreach { a_vec: Vector =>
          val m_vec = a_vec.toArray
          val m_string_vec: Array[String] = new Array[String](m_vec.length)
          for (i <- 0 to m_vec.length - 1) {
            m_string_vec(i) = m_vec(i).toString()
          }
          csv_writer.writeNext(m_string_vec)
        }
      } finally {
        csv_writer.close()
      }
    } catch {
      case e: IOException => throw new IllegalStateException(e)
    }
  }

  /* Wrapper to write to file */
  def write_matrix(filename: String, m_matrix: Matrix): Unit = {
    write_matrix(filename, m_matrix, filename => { new FileWriter(new File(filename)) })
  }

  def write_matrix(filename: String, m_matrix: Matrix, new_writer: (String) => Writer): Unit = {
    // written out as column major matrix
    val mat: BDM[Double] = new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray)
    BCSV.write(new_writer(filename), IndexedSeq.tabulate(mat.rows, mat.cols)(mat(_, _).toString))
  }

  def read_matrix(filename: String): DenseMatrix = {
    read_matrix(() => {
      new BufferedReader(new FileReader(new File(filename)))
    })
  }

  def read_matrix(new_reader: () => Reader): DenseMatrix = {
    val reader: Reader = new_reader()
    try {
      try {
        var mat = BCSVR.read(reader)
        mat = mat.takeWhile(line => line.length != 0 && line.head.nonEmpty) // empty lines at the end
        if (mat.length == 0) {
          val breeze_dense_matrix: BDM[Double] = BDM.zeros[Double](0, 0)
          MLibUtils.fromBreeze(breeze_dense_matrix).asInstanceOf[DenseMatrix]
        } else {
          val breeze_dense_matrix: BDM[Double] = BDM.tabulate(mat.length, mat.head.length)((i, j) => mat(i)(j).toDouble)
          MLibUtils.fromBreeze(breeze_dense_matrix).asInstanceOf[DenseMatrix]
        }
      } finally {
        if (reader != null) {
          reader.close()
        }
      }
    } catch {
      case e: IOException => throw new IllegalStateException(e)
    }
  }

  def read_matrix(client: AmazonS3, bucket_name: String, file_key: String): DenseMatrix = {
    val s3_object: S3Object = client.getObject(bucket_name, file_key)
    read_matrix(() => {
      val in: InputStream = s3_object.getObjectContent
      new BufferedReader(new InputStreamReader(in))
    })
  }

  def read_vectors(filename: String): List[DenseVector] = {
    read_vectors(() => {
      new BufferedReader(new FileReader(filename))
    })
  }

  def read_vectors(new_reader: () => Reader): List[DenseVector] = {
    val csv_reader: CSVReader = new CSVReader(new_reader())
    try {
      try {
        val dlist: ListBuffer[DenseVector] = new ListBuffer[DenseVector]()
        var csv_row: Array[String] = csv_reader.readNext()
        while (csv_row != null) {
          dlist += Vectors.dense(csv_row.map(_.toDouble)).asInstanceOf[DenseVector]
          csv_row = csv_reader.readNext()
        }
        dlist.toList
      } finally {
        csv_reader.close()
      }
    } catch {
      case e: IOException => throw new IllegalStateException(e)
    }
  }

  def read_vectors(client: AmazonS3, bucket_name: String, file_key: String): List[DenseVector] = {
    val s3_object: S3Object = client.getObject(bucket_name, file_key)
    read_vectors(() => {
      val in: InputStream = s3_object.getObjectContent
      new BufferedReader(new InputStreamReader(in))
    })
  }

  /**
   * Process the stream, writes out specified stream to an S3 bucket
   */
  def process_stream(client: AmazonS3, bucket_name: String, output_dir: String, filename: String, stream: ByteArrayOutputStream): Unit = {
    val output_bucket_name = bucket_name
    val simple_filename = filename.split("/").last
    val output_bucket_key = output_dir + simple_filename
    //
    val bytes: Array[Byte] = stream.toByteArray();
    //
    val in_stream: InputStream = new ByteArrayInputStream(bytes)
    val in_stream_meta: ObjectMetadata = new ObjectMetadata()
    in_stream_meta.setContentLength(bytes.length)
    //
    println("Invoking client.putObject with parameters: %s,%s".format(output_bucket_name, output_bucket_key))
    client.putObject(output_bucket_name, output_bucket_key, in_stream, in_stream_meta)
  }

  /**
   * Process the stream, writes out specified stream to a CSV file
   */
  def process_stream(output_dir: String, filename: String, stream: ByteArrayOutputStream): Unit = {
    val dir = new File(output_dir)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    val out: OutputStream = new BufferedOutputStream(new FileOutputStream(new File(filename)))
    try {
      out.write(stream.toByteArray())
    } finally {
      out.close()
    }
  }

  def dump_to_output(target_list: TraversableOnce[Vector], colEnum: PivotColumn, token_name: String, output_parameter: OutputParameter): Unit = {
    val filename_prefix = IOUtils.get_col_prefix(colEnum)
    val fid = output_parameter.m_output_fid
    val output_dir = output_parameter.m_output_dir
    val s3_param = output_parameter.m_s3_param
    //
    val out_filename = output_dir + filename_prefix + token_name + "." + fid + ".csv"
    val out_stream = new ByteArrayOutputStream();
    IOUtils.write_vectors(out_filename, target_list, filename => {
      new BufferedWriter(new OutputStreamWriter(out_stream))
    })
    if (s3_param != null) {
      IOUtils.process_stream(s3_param.m_client, s3_param.m_bucket_name, output_dir, out_filename, out_stream)
    } else {
      IOUtils.process_stream(output_dir, out_filename, out_stream)
    }
  }

}