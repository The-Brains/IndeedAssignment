package com.leobenkel.salaryestimation

import breeze.numerics.abs
import org.apache.spark.ml.linalg
import org.apache.spark.sql.Row

case class Prediction(
  label: Double,
  features: linalg.Vector,
  prediction: Double
) {
  lazy val error: Double = abs(abs(this.prediction) - this.label) / this.label
}

object Prediction {
  def calculateError(r: Row): Double = {
    val prediction = getPrediction(r)
    val label = r.getAs[Double](JobPostingLabeled.LabelCol)

    abs(prediction - label) / label
  }

  def getPrediction(r: Row): Double = {
    r.getAs[Double](JobPostingLabeled.PredictionCol)
  }
}