package com.leobenkel.salaryestimation

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

object SalaryEstimationPipeline {

  def createPipeline: TrainValidationSplit = {
    val lr = new LinearRegression()

    val stringIndexers = JobPostingColumnNames.stringIndexedColumns.map(createStringIndexer).toArray

    val assembler = new VectorAssembler()
      .setInputCols(JobPostingColumnNames.featuresColumns.toArray)
      .setOutputCol(JobPostingColumnNames.FeatureCol)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .build()

    val pipeline = new Pipeline()
      .setStages(stringIndexers ++ Array(assembler, lr))

    val tvs = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)

    tvs
  }

  private def createStringIndexer(inputCol: String): StringIndexer = {
    new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(JobPostingColumnNames.colNameToIndexed(inputCol))
      .setHandleInvalid("keep")
  }
}
