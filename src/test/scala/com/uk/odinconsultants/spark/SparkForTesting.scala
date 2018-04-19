package com.uk.odinconsultants.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkForTesting {

  val master: String          = "local[*]"
  val appName: String         = "Tests"
  val sparkConf: SparkConf    = new SparkConf().setMaster(master).setAppName(appName)
  val sc: SparkContext        = SparkContext.getOrCreate(sparkConf)
  val spark: SparkSession     = SparkSession.builder().getOrCreate()
  val sqlContext: SQLContext  = spark.sqlContext


}
