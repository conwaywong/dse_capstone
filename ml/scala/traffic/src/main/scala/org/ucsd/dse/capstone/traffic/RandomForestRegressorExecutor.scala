package org.ucsd.dse.capstone.traffic

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.SparseVector
import scala.collection.mutable.ListBuffer

/**
 * Executes RandomForestRegressor against specified data
 *
 * @author dyerke
 */
class RandomForestRegressorExecutor(path: String, whiten: Boolean = true) extends Executor[RegressorResult] {

  override def execute(sc: SparkContext, sql_context: SQLContext, args: String*): RegressorResult = {
    //
    // create DataFrame
    //
    val (df, feature_column_name, label_column_name, column_names) = IOUtils.parse_preprocessed(sc, sql_context, path, whiten)
    //
    // execute RandomForestRegressor
    //
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
    val regressor: RandomForestRegressor = new RandomForestRegressor()
    regressor.setLabelCol(label_column_name).setFeaturesCol(feature_column_name)
    //
    // Train
    //
    val model: RandomForestRegressionModel = regressor.fit(trainingData)
    //
    // Predict
    //
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", label_column_name, feature_column_name).show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new RegressionEvaluator()
      .setLabelCol(label_column_name)
      .setPredictionCol("prediction")
      .setMetricName("r2")
    //
    val score = evaluator.evaluate(predictions)
    println("R2 Score on test data = " + score)
    //
    val out_list: ListBuffer[Tuple2[String, Double]] = new ListBuffer[Tuple2[String, Double]]()
    val fi: SparseVector = model.featureImportances.asInstanceOf[SparseVector]
    fi.foreachActive { (a_idx, a_val) =>
      val feature_name: String = column_names(a_idx)
      val feature_rate: Double = a_val
      val new_entry: Tuple2[String, Double] = (feature_name, feature_rate)
      out_list += new_entry
    }
    val sorted_feature_importance_list = out_list.sortWith { (left, right) =>
      left._2 > right._2
    }.toList
    return new RegressorResult(score, sorted_feature_importance_list)
  }
}