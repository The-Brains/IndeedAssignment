package com.leobenkel.salaryestimation

import com.thebrains.sparkcommon.{SparkJob, SparkMain}
import com.thebrains.utils.{Config, ReadData}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Normalizer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.io.File

/*
* useful website:
*   https://www.datacamp.com/community/tutorials/apache-spark-tutorial-machine-learning
*/

case class SalaryEstimationTask(spark: SparkSession)
  extends SparkJob[SalaryEstimationConfig](spark) {

  private def getTrainingLabel(jobPosting: Dataset[JobPosting]): Dataset[JobData] = {
    import spark.implicits._
    val filename: String = "train_salaries_2013-03-07.csv"
    val pathToFile: String = File(System.getProperty("user.dir") + s"/src/main/resources/$filename")
      .toURL
      .getPath

    val salaryData = ReadData.readCSV[JobSalary](pathToFile)
      .filter(_.salary > 0)

    jobPosting
      .join(salaryData, JobPostingLabeled.JobId)
      .as[JobData]
  }

  private def getTrainingData: Dataset[JobPosting] = {
    val filename: String = "train_features_2013-03-07.csv"
    val pathToFile: String = File(System.getProperty("user.dir") + s"/src/main/resources/$filename")
      .toURL
      .getPath

    ReadData.readCSV[JobPosting](pathToFile)
      .filter(j => j.jobType != null
        && j.degree != null
        && j.major != null
        && j.industry != null
        && j.jobId != null
      )
    //      .filter(j =>
    //        j.milesFromMetropolis < JobPostingLabeled.MaxMetropolisDistance
    //          && j.yearsExperience <= JobPostingLabeled.MaxYearExperience
    //      )
    //      .cache
  }

  //  private def turnColumnToIndex(
  //    jobPostings: RDD[JobPosting],
  //    getColumn: (JobPosting) => String
  //  ): Broadcast[Map[String, Double]] = {
  //    spark.sparkContext.broadcast(jobPostings
  //      .map(j => getColumn(j).toLowerCase())
  //      .distinct
  //      .sortBy(identity)
  //      .zipWithUniqueId
  //      .mapValues(_.toDouble)
  //      .collect
  //      .toMap
  //    )
  //  }

  //  private def convertToLabeledPoint(
  //    trainingData: RDD[(JobPosting, JobSalary)],
  //    mapJobType: Broadcast[Map[String, Double]],
  //    mapDegrees: Broadcast[Map[String, Double]],
  //    mapIndustry: Broadcast[Map[String, Double]],
  //    mapMajor: Broadcast[Map[String, Double]]
  //  ): RDD[LabeledPoint] = {
  //    trainingData
  //      .mapPartitions { case (jobs: Iterator[(JobPosting, JobSalary)]) =>
  //        val mapJobTypeUnpacked = mapJobType.value
  //        val mapDegreesUnpacked = mapDegrees.value
  //        val mapIndustryUnpacked = mapIndustry.value
  //        val mapMajorUnpacked = mapMajor.value
  //        jobs
  //          .map { case (jobPosting, jobSalary) =>
  //            JobPostingLabeled.toLabeledPoint(
  //              jobPosting,
  //              jobSalary,
  //              mapJobTypeUnpacked,
  //              mapDegreesUnpacked,
  //              mapMajorUnpacked,
  //              mapIndustryUnpacked
  //            )
  //          }
  //      }
  //  }

  private def createPipeline2: TrainValidationSplit = {
    val lr = new LinearRegression()

    val stringIndexers = JobPostingLabeled.stringIndexedColumns.map(createStringIndexer).toArray

    val assembler = new VectorAssembler()
      .setInputCols(JobPostingLabeled.featuresColumns.toArray)
      .setOutputCol(JobPostingLabeled.FeatureCol)

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

  private def createPipeline: Pipeline = {
    val pipeline = new Pipeline()

    val stringIndexers = JobPostingLabeled.stringIndexedColumns.map(createStringIndexer).toArray

    val assembler = new VectorAssembler()
      .setInputCols(JobPostingLabeled.featuresColumns.toArray)
      .setOutputCol(JobPostingLabeled.FeatureCol)

    // createScaler(JobPostingLabeled.FeatureCol)
    val scalers = new Normalizer()
      .setInputCol(JobPostingLabeled.FeatureCol)
      .setOutputCol(JobPostingLabeled.colNameToScaled(JobPostingLabeled.FeatureCol))

    val lr = new LinearRegression()
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.1)
      .setFitIntercept(false)
      .setFeaturesCol(JobPostingLabeled.colNameToScaled(JobPostingLabeled.FeatureCol))
      //      .setFeaturesCol(JobPostingLabeled.FeatureCol)
      .setLabelCol(JobPostingLabeled.SalaryCol)
      .setPredictionCol(JobPostingLabeled.PredictionCol)

    val stages: Array[PipelineStage] = stringIndexers ++ Seq(assembler, scalers, lr)

    pipeline.setStages(stages)
  }

  //  private def createScaler(inputCol: String): StandardScaler = {
  //    new StandardScaler()
  //      .setInputCol(inputCol)
  //      .setOutputCol(JobPostingLabeled.colNameToScaled(inputCol))
  //    //      .setWithStd(true)
  //    //      .setWithMean(true)
  //  }

  private def createStringIndexer(inputCol: String): StringIndexer = {
    new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(JobPostingLabeled.colNameToIndexed(inputCol))
      .setHandleInvalid("keep")
  }

  override def run(config: SalaryEstimationConfig): Unit = {
    import spark.implicits._

    val jobPostings = getTrainingData.repartition(6).cache
    //      .sample(withReplacement = true, fraction = 0.01, seed = 0)
    log.info(s"Got training data (${jobPostings.count})")
    //    val mapJobType = turnColumnToIndex(jobPostings, _.jobType)
    //    val mapDegrees = turnColumnToIndex(jobPostings, _.degree)
    //    val mapIndustry = turnColumnToIndex(jobPostings, _.industry)
    //    val mapMajor = turnColumnToIndex(jobPostings, _.major)
    //    val mapYear = turnColumnToIndex(jobPostings, _.yearsExperience.toString)
    //    val mapMiles = turnColumnToIndex(jobPostings, _.milesFromMetropolis.toString)

    //    log.info(s"${mapJobType.value.size} different job type found: ${mapJobType.value.keys.toSeq.sortBy(identity).mkString(", ")}")
    //    log.info(s"${mapDegrees.value.size} different degree found: ${mapDegrees.value.keys.toSeq.sortBy(identity).mkString(", ")}")
    //    log.info(s"${mapIndustry.value.size} different industry found: ${mapIndustry.value.keys.toSeq.sortBy(identity).mkString(", ")}")
    //    log.info(s"${mapMajor.value.size} different major found: ${mapMajor.value.keys.toSeq.sortBy(identity).mkString(", ")}")
    //    log.info(s"${mapYear.value.size} different Year found: ${mapYear.value.keys.toSeq.sortBy(identity).mkString(", ")}")
    //    log.info(s"${mapMiles.value.size} different Miles found: ${mapMiles.value.keys.toSeq.sortBy(identity).mkString(", ")}")

    val trainingData: Dataset[JobData] = getTrainingLabel(jobPostings).repartition(6).cache
    //
    //      .map { case (jobPosting: JobPosting, jobSalary: JobSalary) =>
    //        JobPostingLabeled.toRow(jobPosting, jobSalary)
    //      }
    log.info(s"Got training label. Data: ${trainingData.count}")

    //    val labeledPoints: RDD[LabeledPoint] = convertToLabeledPoint(
    //      trainingData,
    //      mapJobType,
    //      mapDegrees,
    //      mapIndustry,
    //      mapMajor
    //    )

    val labeledPoints = trainingData
      //      .map(_.reduceSalary)
      .map(_.toLowerCase)
      .withColumnRenamed(JobPostingLabeled.SalaryCol, JobPostingLabeled.LabelCol)

    log.info("Converted to dataframe")

    log.info("Creating pipeline and splitting data")

    val Array(trainingSet, testSet) = labeledPoints.randomSplit(
      Array[Double](0.9, 0.1),
      seed = 1990
    )

    val pipeline = createPipeline2 //createPipeline

    log.info("Fit model")
    val model = pipeline.fit(trainingSet)

    log.info("Done fitting model")

//    val ml = model.stages.last.asInstanceOf[LinearRegressionModel]
//    val coefficients = ml.coefficients
//    log.info(s"Coefficients: ${coefficients.toArray.mkString(", ")}")
//    val intercept = ml.intercept
//    log.info(s"Intercept: $intercept")
//    val summary = ml.summary
    /*
    * The RMSE measures how much error there is between two datasets comparing a predicted value
    * and an observed or known value. The smaller an RMSE value, the closer predicted and
    * observed values are.
    */
//    log.info(s"Root mean square: ${summary.rootMeanSquaredError}")
    /*
    * The R2 (“R squared”) or the coefficient of determination is a measure that shows how close
    * the data are to the fitted regression line. This score will always be between 0 and a 100%
    * (or 0 to 1 in this case), where 0% indicates that the model explains none of the
    * variability of the response data around its mean, and 100% indicates the opposite:
    * it explains all the variability. That means that, in general, the higher the R-squared,
    * the better the model fits your data.
    */
//    log.info(s"R2: ${summary.r2}")

    val results = model.transform(testSet).cache

    val rm = new RegressionMetrics(results.rdd.map(x =>
      (x.getAs[Double](JobPostingLabeled.PredictionCol),
        x.getAs[Double](JobPostingLabeled.LabelCol)
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
      .select(JobPostingLabeled.LabelCol, JobPostingLabeled.PredictionCol)
      .take(4)
      .map(_.toString)
      .mkString(", "))

    val delta: Dataset[Double] = results.map(Prediction.calculateError)
    val minError: Double = delta.reduce((a, b) => Math.min(a, b))
    val averageError: Double = delta.reduce((a, b) => a + b) / resultCount
    val maxError: Double = delta.reduce((a, b) => Math.max(a, b))
    log.info(s"Min Error: $minError")
    log.info(s"Average Error: $averageError")
    log.info(s"Max Error: $maxError")

    val salaries: Dataset[Double] = results.map(Prediction.getPrediction)
    val minSalary: Double = salaries.reduce((a, b) => Math.min(a, b))
    val averageSalary: Double = salaries.reduce((a, b) => a + b) / resultCount
    val maxSalary: Double = salaries.reduce((a, b) => Math.max(a, b))
    log.info(s"Min Salary: $minSalary")
    log.info(s"Average Salary: $averageSalary")
    log.info(s"Max Salary: $maxSalary")

    //    val scalerModel = scaler.fit(labeledPoints.toDF)
    //    val labeledPointsScaled = scalerModel.transform(labeledPoints.toDF).toDF().as[LabeledPoint].rdd
    //    val model = lr.fit(trainingSet.toDF)

    //    log.info(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")
    //
    //    // Summarize the model over the training set and print out some metrics
    //    val trainingSummary = model.summary
    //    log.info(s"numIterations: ${trainingSummary.totalIterations}")
    //    log.info(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //    //    log.info(s"Coefficient Standard Errors: ${trainingSummary.coefficientStandardErrors.mkString(",")}")
    //    //    log.info(s"T Values: ${trainingSummary.tValues.mkString(",")}")
    //    log.info(s"P Values: ${trainingSummary.pValues.mkString(",")}")
    //    log.info("Deviance Residuals: ")
    //    trainingSummary.residuals.show()
    //    log.info(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //    log.info(s"r2: ${trainingSummary.r2}")
    //
    //
    //    val summary = model.evaluate(testSet.toDF)
    //    log.info(s"Summary: ${summary.degreesOfFreedom} | ${summary.explainedVariance} | " +
    //      s"${summary.meanAbsoluteError} | ${summary.meanSquaredError}")
    //
    //    //    log.info(s"Dispersion: ${summary.dispersion}")
    //    //    log.info(s"Null Deviance: ${trainingSummary.nullDeviance}")
    //    //    log.info(s"Residual Degree Of Freedom Null: ${trainingSummary.residualDegreeOfFreedomNull}")
    //    //    log.info(s"Deviance: ${trainingSummary.deviance}")
    //    //    log.info(s"Residual Degree Of Freedom: ${trainingSummary.residualDegreeOfFreedom}")
    //    //    log.info(s"AIC: ${trainingSummary.aic}")

    //    val predictions = model.transform(testSet.map(t => t.features.toArray).toDF)
    //      .toDF
    //      .as[Prediction]
    //      .rdd
  }
}


object SalaryEstimationMain extends SparkMain[SalaryEstimationTask, SalaryEstimationConfig] {
  override def getName: String = "SalaryEstimation"

  override protected def instantiateJob: SparkSession => SalaryEstimationTask = {
    (spark: SparkSession) => SalaryEstimationTask(spark)
  }

  override protected def instantiateConfig: Seq[String] => SalaryEstimationConfig = {
    (args: Seq[String]) => SalaryEstimationConfig(args)
  }
}

case class SalaryEstimationConfig(override val args: Seq[String]) extends Config(args) {
  //  val test = opt[Int](required = true)
}