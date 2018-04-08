package com.leobenkel.salaryestimation

object JobPostingColumnNames {
  val JobId: String = "JobId"
  val CompanyCol: String = "companyId"
  val IndustryCol: String = "industry"
  val MajorCol: String = "major"
  val DegreeCol: String = "degree"
  val JobTypeCol: String = "jobType"
  val DistanceCol: String = "milesFromMetropolis"
  val YearsCol: String = "yearsExperience"
  val SalaryCol: String = "salary"
  val LabelCol: String = "label"
  val FeatureCol: String = "features"
  val PredictionCol: String = "prediction"

  val stringIndexedColumns: Seq[String] = Seq(
    CompanyCol,
    JobTypeCol,
    DegreeCol,
    MajorCol,
    IndustryCol
  )

  val featuresColumns: Seq[String] =
    stringIndexedColumns.map(colNameToIndexed) ++ Seq(
      YearsCol,
      DistanceCol
    )

  def colNameToIndexed(name: String): String = s"${name}Vec"

  def colNameToScaled(name: String): String = s"${name}Scaled"

}
