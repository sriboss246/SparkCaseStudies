package com.dataing.workflow

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties

class SparkInit() {

  def getSparkSession(sparkParams : Map[String,String]) : SparkSession = {
    var sparkSession : SparkSession = null
    val executionEnvType = sparkParams.get("executionEnvironment")
    if(executionEnvType.get.toUpperCase().equals("LOCAL")){
         sparkSession = SparkSession.builder().master("local[*]").config(getSparkConf(sparkParams)).getOrCreate()
    }
    else {
     sparkSession = SparkSession.builder().config(getSparkConf(sparkParams)).getOrCreate()
    }
    sparkSession
  }

  def getSparkConf(params : Map[String,String]) : SparkConf = {
    val conf : SparkConf = new SparkConf()
    params.keys.map(key => conf.set(key,params.get(key).get))

    conf
  }

}
