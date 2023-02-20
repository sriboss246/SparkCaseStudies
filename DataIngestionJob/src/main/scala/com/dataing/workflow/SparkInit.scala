package com.dataing.workflow

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

class SparkInit() {

  def getSparkSession(sparkParams : Map[String,String]) : SparkSession = {

    val sparkSession = SparkSession.builder().config(getSparkConf(sparkParams)).getOrCreate()

    sparkSession
  }

  def getSparkConf(params : Map[String,String]) : SparkConf = {
    val conf : SparkConf = new SparkConf()
    params.keys.map(key => conf.set(key,params.get(key).toString))

    conf
  }

}
