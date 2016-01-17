import java.io.File
import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Calendar
import java.util.TimeZone
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.TreeSet
import scala.math.Ordering
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.MatrixUDT
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.SQLUserDefinedType
import breeze.linalg.{ DenseMatrix => BDM }
import breeze.linalg.{ DenseVector => BDV }
import breeze.linalg.csvwrite
import breeze.linalg.{ svd => brzSvd }
import org.apache.commons.math3.stat.descriptive.MultivariateSummaryStatistics
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.mllib.feature.StandardScaler

/**
 * @author dyerke
 */
object SparkMLib {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMLibPCA")
    val sc = new SparkContext(conf)
    //
    val m_file_name = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/01_2010"
    //val m_file_name = "/home/dyerke/Documents/DSE/capstone_project/traffic/data/d11_text_station_5min_2015_01_01.txt"
    val fid = m_file_name.split('/').last
    //
    val lines: RDD[String] = sc.textFile(m_file_name, 4)
    val t_rdd: RDD[(String, ListBuffer[Any])] = lines.map(m_map_row)
    val pair_rdd: PairRDDFunctions[String, ListBuffer[Any]] = RDD.rddToPairRDDFunctions(t_rdd)
    val m_result_rdd: RDD[(String, Iterable[ListBuffer[Any]])] = pair_rdd.groupByKey()
    val m_vector_rdd: RDD[Vector] = m_result_rdd.map(m_map_vector).filter { v => v.size > 0 }
    // obtain mean vector
    val m_summary_stats: MultivariateStatisticalSummary = Statistics.colStats(m_vector_rdd)
    val mean_vector = m_summary_stats.mean.toArray
    val mean_filename= "/tmp/mean_vector." + fid + ".csv"
    write_vector(mean_filename, mean_vector)
    // normalize dataset
    val scaler: StandardScaler= new StandardScaler()
    val m_norm_vector_rdd: RDD[Vector]= scaler.fit(m_vector_rdd).transform(m_vector_rdd)
    // execute PCA against normalized dataset
    val k = 30
    val (eigenvectors, eigenvalues) = computePrincipalComponentsAndExplainedVariance(m_norm_vector_rdd, k)
    //
    // eigenvectors written out as column-major matrix
    //
    val eigenvectors_filename= "/tmp/eigenvectors." + fid + ".csv"
    write_matrix(eigenvectors_filename, eigenvectors)
    //
    // eigenvalues written out as one row
    //
    val eigenvalue_filename= "/tmp/eigenvalues." + fid + ".csv"
    write_vector(eigenvalue_filename, eigenvalues.toArray)
    //
    // print statements to verify
    //
    println("eigenvectors= " + eigenvectors)
    println("eigenvalues= " + eigenvalues)
    val m_list_buffer = new ListBuffer[Double]()
    val m_eig_arr: Array[Double] = eigenvalues.toArray
    var cum_sum = 0.0
    for (i <- 0 to m_eig_arr.length - 1) {
      cum_sum += m_eig_arr(i)
      m_list_buffer += cum_sum
    }
    println("perc variance explained= " + m_list_buffer)
  }

  private def write_vector(filename: String, m_vector: Array[Double]) = {
    // written out as one row
    val arr_values = (new BDM[Double](m_vector.length, 1, m_vector)).t
    csvwrite(new File(filename), arr_values)
  }

  private def write_matrix(filename: String, m_matrix: Matrix) = {
    // written out as column major matrix
    csvwrite(new File(filename), new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray))
  }

  private def m_map_row(line: String): (String, ListBuffer[Any]) = {
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
      resultList += x_arr(7).toDouble
      resultList += x_arr(8).toDouble
      resultList += x_arr(9).toDouble
      resultList += x_arr(10).toDouble
      resultList += x_arr(11).toDouble
      //
      (key_builder.toString(), resultList)
    } else {
      ("", new ListBuffer())
    }
  }

  private def m_map_vector(tuple: Tuple2[String, Iterable[ListBuffer[Any]]]): Vector = {
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
      set.foreach { list => doubles_list += list(2).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(3).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(4).asInstanceOf[Double] }
      set.foreach { list => doubles_list += list(5).asInstanceOf[Double] }
      Vectors.dense(doubles_list.toArray)
    } else {
      Vectors.zeros(0)
    }
  }

  def computePrincipalComponentsAndExplainedVariance(m_vector_rdd: RDD[Vector], k: Int): (Matrix, Vector) = {
    val row_matrix: RowMatrix = new RowMatrix(m_vector_rdd)
    val n = row_matrix.numCols().toInt
    require(k > 0 && k <= n, s"k = $k out of range (0, n = $n]")

    val Cov: BDM[Double] = toBreeze(row_matrix.computeCovariance())

    val brzSvd.SVD(u: BDM[Double], s: BDV[Double], _) = brzSvd(Cov)

    val eigenSum = s.data.sum
    val explainedVariance = s.data.map(_ / eigenSum)

    if (k == n) {
      (Matrices.dense(n, k, u.data), Vectors.dense(explainedVariance))
    } else {
      (Matrices.dense(n, k, Arrays.copyOfRange(u.data, 0, n * k)),
        Vectors.dense(Arrays.copyOfRange(explainedVariance, 0, k)))
    }
  }

  private def toBreeze(m_matrix: Matrix): BDM[Double] = {
    if (!m_matrix.isTransposed) {
      new BDM[Double](m_matrix.numRows, m_matrix.numCols, m_matrix.toArray)
    } else {
      val breezeMatrix = new BDM[Double](m_matrix.numCols, m_matrix.numRows, m_matrix.toArray)
      breezeMatrix.t
    }
  }
}