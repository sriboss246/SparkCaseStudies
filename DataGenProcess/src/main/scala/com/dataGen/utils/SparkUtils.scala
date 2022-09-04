package com.dataGen.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

class SparkUtils {

  var spark : SparkSession = null
  def sparkInit(appName : String):SparkSession ={

    spark = SparkSession.builder().appName(appName)
      .enableHiveSupport().getOrCreate()

    spark
  }

  def sparkInitLocal(appName : String) : SparkSession={

    //System.setProperty("hadoop.home.dir","D:\\Hadoop_WinUtils\\winutils-master\\hadoop-2.6.0")
    spark=SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      //.enableHiveSupport()
      //.config("spark.sql.warehouse.dir", "D:\\tmp\\hive")
      .getOrCreate();

    spark
  }

  def sparkStreaminit():StreamingContext= {

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    ssc
  }

}
