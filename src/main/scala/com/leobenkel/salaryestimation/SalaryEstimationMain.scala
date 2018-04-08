package com.leobenkel.salaryestimation

import com.thebrains.sparkcommon.SparkMain
import com.thebrains.utils.Config
import org.apache.spark.sql.SparkSession


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
}