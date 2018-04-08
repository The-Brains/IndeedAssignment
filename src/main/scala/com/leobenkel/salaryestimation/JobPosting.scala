package com.leobenkel.salaryestimation

import breeze.numerics.abs
import org.apache.spark.sql.Row

case class JobPosting(
  jobId: String,
  companyId: String,
  jobType: String,
  degree: String,
  major: String,
  industry: String,
  yearsExperience: Int,
  milesFromMetropolis: Int
)

case class JobSalary(
  jobId: String,
  salary: Long
)

case class JobData(
  jobId: String,
  companyId: String,
  jobType: String,
  degree: String,
  major: String,
  industry: String,
  yearsExperience: Double,
  milesFromMetropolis: Double,
  salary: Double
) {
  def toLowerCase: JobData = {
    this.copy(
      jobId = this.jobId.toLowerCase.trim,
      companyId = this.companyId.toLowerCase.trim,
      jobType = this.jobType.toLowerCase.trim,
      degree = this.degree.toLowerCase.trim,
      major = this.major.toLowerCase.trim,
      industry = this.industry.toLowerCase.trim
    )
  }
}

object JobPostingUtility {
  def calculateError(r: Row): Double = {
    val prediction = getPrediction(r)
    val label = r.getAs[Double](JobPostingColumnNames.LabelCol)

    abs(prediction - label) / label
  }

  def getPrediction(r: Row): Double = {
    r.getAs[Double](JobPostingColumnNames.PredictionCol)
  }
}