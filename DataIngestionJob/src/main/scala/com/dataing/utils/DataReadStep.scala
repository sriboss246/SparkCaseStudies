package com.dataing.utils
import com.dataing.beans.{DataFileBean, DataMappingBean, DataTableBean, InputBean}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.dataing.commons.AppConstants
import org.apache.hadoop.fs.Path
class DataReadStep  {

 def readDataToDataframe(inputBean: InputBean, sparkSession: SparkSession) : DataFrame = {

   val inputType = inputBean.inputType

   val inputDataFrame = inputType match {
     case "FILE" => {val dataFileBean = inputBean.fileBean
                        readFileToDataframe(dataFileBean,sparkSession)}
     case "DB" =>  {val dataTableBean = inputBean.dataTableBean
                           readDBToDataframe(dataTableBean,sparkSession)}
   }

   return inputDataFrame

 }

  def readFileToDataframe(dataFileBean: DataFileBean, sparkSession: SparkSession): DataFrame ={

    val sourceType = dataFileBean.fileType
    val headerExist = dataFileBean.headerExist
    val fileType = dataFileBean.fileType
    val filePath = dataFileBean.location

    val fileInputDataFrame = fileType match {
      case "csv" =>  sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").option("header",if(headerExist == true )  "true" else "false").csv(filePath)
      case "json" => sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").json(filePath)
      case "parquet" => sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").parquet(filePath)
      case "text" => sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").option("header",if(headerExist == true )  "true" else "false").text(filePath)
      case _ => throw new Exception()
    }
    return fileInputDataFrame;
  }

  def readDBToDataframe(dataTableBean: DataTableBean, sparkSession: SparkSession) : DataFrame ={

      val resultDataframe = sparkSession.read.format("jdbc").option("driver",dataTableBean.driver)
                            .option("url",dataTableBean.url)
                            .option("dbTable",dataTableBean.tableName)
                            .option("username",dataTableBean.userName)
                            .option("password",dataTableBean.password).load().where(dataTableBean.filterString)

    return resultDataframe
  }


  /*def validateFilePath(dataFileBean : DataFileBean, sparkSession: SparkSession) : Boolean = {

    val sourceType = dataFileBean.fileType
    val filePath = dataFileBean.location

    val fileExists = sourceType match {
      case "hdfs" => { val conf = sparkSession.sparkContext.hadoopConfiguration
                        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
                        val fileExists = fs.exists(new Path(filePath))
                        fileExists }

      case "s3" => {
        val fileExists = fs.exists(new Path(filePath))
        fileExists }
    }



  }*/



}
