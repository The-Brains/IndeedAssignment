package com.leobenkel.salaryestimation

import com.thebrains.utils.IoData
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait SalaryEstimationSources {
  private def getData[T <: Product : ClassTag : TypeTag](
    filename: String
  )(implicit spark: SparkSession): Dataset[T] = {
    val pathToFile: String = IoData.getResourcePath(filename)

    IoData.readCsv[T](pathToFile)
  }

  def getTrainingLabel(
    jobPosting: Dataset[JobPosting]
  )(implicit spark: SparkSession): Dataset[JobData] = {
    import spark.implicits._

    val filename: String = "train_salaries_2013-03-07.csv"
    val salaryData = getData[JobSalary](filename)
      .filter(_.salary > 0)

    jobPosting
      .join(salaryData, JobPostingColumnNames.JobId)
      .as[JobData]
  }

  private def getJobPosting(
    filename: String
  )(implicit spark: SparkSession): Dataset[JobPosting] = {
    getData[JobPosting](filename)
      .filter(j => j.jobType != null
        && j.degree != null
        && j.major != null
        && j.industry != null
        && j.jobId != null
      )
  }

  def getTrainingData(implicit spark: SparkSession): Dataset[JobPosting] = {
    val filename: String = "train_features_2013-03-07.csv"
    getJobPosting(filename)
  }

  def getTestData(implicit spark: SparkSession): Dataset[JobPosting] = {
    val filename: String = "test_features_2013-03-07.csv"
    getJobPosting(filename)
  }
}
