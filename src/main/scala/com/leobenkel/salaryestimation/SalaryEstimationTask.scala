package com.leobenkel.salaryestimation

import com.thebrains.sparkcommon.SparkJob
import com.thebrains.utils.IoData
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class SalaryEstimationTask(spark: SparkSession)
  extends SparkJob[SalaryEstimationConfig](spark)
    with SalaryEstimationSources {

  private def evaluateModel(model: TrainValidationSplitModel, testingSet: DataFrame): Unit = {
    import spark.implicits._

    val results = model.transform(testingSet).cache

    val rm = new RegressionMetrics(results.rdd.map(x =>
      (x.getAs[Double](JobPostingColumnNames.PredictionCol),
        x.getAs[Double](JobPostingColumnNames.LabelCol)
      )
    ))

    log.info("Test Metrics")
    log.info("Test Explained Variance:")
    log.info(rm.explainedVariance)
    log.info("Test R^2 Coef:")
    log.info(rm.r2)
    log.info("Test MSE:")
    log.info(rm.meanSquaredError)
    log.info("Test RMSE:")
    log.info(rm.rootMeanSquaredError)

    val resultCount = results.count

    log.info(s"Got $resultCount results")
    log.info("Sample of results: ")
    log.info(results
      .sample(0.01)
      .select(JobPostingColumnNames.LabelCol, JobPostingColumnNames.PredictionCol)
      .take(4)
      .map(_.toString)
      .mkString(", "))

    val delta: Dataset[Double] = results.map(JobPostingUtility.calculateError)
    val minError: Double = delta.reduce((a, b) => Math.min(a, b))
    val averageError: Double = delta.reduce((a, b) => a + b) / resultCount
    val maxError: Double = delta.reduce((a, b) => Math.max(a, b))
    log.info(s"Min Error: $minError")
    log.info(s"Average Error: $averageError")
    log.info(s"Max Error: $maxError")

    val salaries: Dataset[Double] = results.map(JobPostingUtility.getPrediction)
    val minSalary: Double = salaries.reduce((a, b) => Math.min(a, b))
    val averageSalary: Double = salaries.reduce((a, b) => a + b) / resultCount
    val maxSalary: Double = salaries.reduce((a, b) => Math.max(a, b))
    log.info(s"Min Salary: $minSalary")
    log.info(s"Average Salary: $averageSalary")
    log.info(s"Max Salary: $maxSalary")
  }

  private def assembleData: DataFrame = {
    import spark.implicits._

    val jobPostings = getTrainingData.repartition(6).cache
    log.info(s"Got training data (${jobPostings.count})")

    val trainingData: Dataset[JobData] = getTrainingLabel(jobPostings).repartition(6).cache
    log.info(s"Got training label. Data: ${trainingData.count}")

    trainingData
      .map(_.toLowerCase)
      .withColumnRenamed(JobPostingColumnNames.SalaryCol, JobPostingColumnNames.LabelCol)
  }

  override def run(config: SalaryEstimationConfig): Unit = {
    val labeledPoints = assembleData

    log.info("Splitting data")
    val Array(trainingSet, testSet) = labeledPoints.randomSplit(
      Array[Double](0.9, 0.1),
      seed = 1990
    )

    val pipeline = SalaryEstimationPipeline.createPipeline

    log.info("Train model")
    val model = pipeline.fit(trainingSet)

    evaluateModel(model, testSet)

    val dataToEstimate = getTestData
    val results = model.transform(dataToEstimate)
      .select(JobPostingColumnNames.JobId, JobPostingColumnNames.PredictionCol)

    val outputPath = IoData.getTargetPath("test_salaries_2013-03-07.csv")
    IoData.writeCsv(
      results
        .withColumnRenamed(JobPostingColumnNames.PredictionCol, JobPostingColumnNames.SalaryCol),
      outputPath
    )
  }
}
