package com.leobenkel.salaryestimation

case class JobPosting(
  jobId: String,
  companyId: String,
  jobType: String,
  degree: String,
  major: String,
  industry: String,
  yearsExperience: Int,
  milesFromMetropolis: Int
) extends Serializable {
  //  def toVector(
  //    mapJobType: Map[String, Double],
  //    mapDegree: Map[String, Double],
  //    mapMajor: Map[String, Double],
  //    mapIndustry: Map[String, Double]
  //  ): linalg.Vector = {
  //    Vectors.dense(
  //      Seq(
  //        mapJobType(this.jobType.toLowerCase) / mapJobType.size,
  //        mapDegree(this.degree.toLowerCase) / mapDegree.size,
  //        mapMajor(this.major.toLowerCase) / mapMajor.size,
  //        mapIndustry(this.industry.toLowerCase) / mapIndustry.size,
  //        this.yearsExperience.toDouble / JobPostingLabeled.MaxYearExperience,
  //        this.milesFromMetropolis.toDouble / JobPostingLabeled.MaxMetropolisDistance
  //      )
  //        .toArray
  //    )
  //  }
}

case class JobSalary(
  jobId: String,
  salary: Long
) extends Serializable {
  lazy val smallerSalary: Double = salary / 100.0
}

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

  //  def splitYearOfExperience: JobData = {
  //    this.copy(yearsExperience = if (this.yearsExperience.toInt <= 5) {
  //      "JUNIOR"
  //    } else if (this.yearsExperience.toInt <= 10) {
  //      "SENIOR"
  //    } else {
  //      "EXPERT"
  //    })
  //  }
  //
  //  def splitMilesFromMetropolis: JobData = {
  //    this.copy(milesFromMetropolis = if (this.milesFromMetropolis.toInt <= 10) {
  //      "CLOSE"
  //    } else if (this.milesFromMetropolis.toInt <= 25) {
  //      "NEARBY"
  //    } else {
  //      "FAR"
  //    })
  //}

  def reduceSalary: JobData = {
    this.copy(salary = this.salary / 100.0)
  }
}

object JobPostingLabeled extends Serializable {
  val MaxYearExperience: Int = 25
  val MaxMetropolisDistance: Int = 100

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
    //    YearsCol,
    //    DistanceCol
  )

  //  val scaledColumns: Seq[String] = Seq(
  //    DistanceCol
  //  )

  val featuresColumns: Seq[String] =
    stringIndexedColumns.map(colNameToIndexed) ++ Seq(
      YearsCol,
      DistanceCol
    )

  def colNameToIndexed(name: String): String = s"${name}Vec"

  def colNameToScaled(name: String): String = s"${name}Scaled"

  //  def toLabeledPoint(
  //    jobPosting: JobPosting,
  //    jobSalary: JobSalary,
  //    mapJobType: Map[String, Double],
  //    mapDegree: Map[String, Double],
  //    mapMajor: Map[String, Double],
  //    mapIndustry: Map[String, Double]
  //  ): LabeledPoint = {
  //    val label: Double = jobSalary.smallerSalary
  //    val vector: linalg.Vector = jobPosting.toVector(
  //      mapJobType,
  //      mapDegree,
  //      mapMajor,
  //      mapIndustry
  //    )
  //    LabeledPoint(
  //      label = label,
  //      features = vector
  //    )
  //  }

  //  def toRow(r: Row): Row = {
  //    toRow(
  //      r.getStruct(0).asInstanceOf[JobPosting],
  //      r.getStruct(1).asInstanceOf[JobSalary]
  //    )
  //  }
  //
  //  def toRow(
  //    jobData: JobData
  //  ): Row = {
  //    Row(
  //      IndustryCol -> jobData.industry,
  //      MajorCol -> jobData.major,
  //      DegreeCol -> jobData.degree,
  //      JobTypeCol -> jobData.jobType,
  //      DistanceCol -> jobData.milesFromMetropolis,
  //      YearsCol -> jobData.yearsExperience,
  //      SalaryCol -> jobData.salary
  //    )
  //  }
  //
  //  def toRow(
  //    jobPosting: JobPosting,
  //    jobSalary: JobSalary
  //  ): Row = {
  //    Row(
  //      IndustryCol -> jobPosting.industry,
  //      MajorCol -> jobPosting.major,
  //      DegreeCol -> jobPosting.degree,
  //      JobTypeCol -> jobPosting.jobType,
  //      DistanceCol -> jobPosting.milesFromMetropolis,
  //      YearsCol -> jobPosting.yearsExperience,
  //      SalaryCol -> jobSalary.salary
  //    )
  //  }
}