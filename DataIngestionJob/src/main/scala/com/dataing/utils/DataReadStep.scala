package com.dataing.utils
import com.dataing.beans.{DataFileBean, DataMappingBean, DataTableBean, InputBean}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.dataing.commons.AppConstants
import org.apache.hadoop.fs.Path

import java.util.Properties
class DataReadStep  {

 def readDataToDataframe(inputBean: InputBean, sparkSession: SparkSession) : DataFrame = {

   val inputType = inputBean.inputType

   val inputDataFrame = inputType match {
     case "FILE" => {val dataFileBean = inputBean.dataFileBean
                        readFileToDataframe(dataFileBean,sparkSession)}
     case "DB" =>  {val dataTableBean = inputBean.dataTableBean
                           readDBToDataframe(dataTableBean,sparkSession)}
   }

   return inputDataFrame

 }

  def readFileToDataframe(dataFileBean: DataFileBean, sparkSession: SparkSession): DataFrame ={


    val headerExist = dataFileBean.headerExist
    val fileType = dataFileBean.fileType
    val filePath = dataFileBean.location+"/"+dataFileBean.fileName
    val delimiter = dataFileBean.delimiter

    val fileInputDataFrame = fileType match {
      case "csv" => if(delimiter!="" || !delimiter.isEmpty || !delimiter.isBlank )  {
                    sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").option("header",if(headerExist == true )  "true" else "false").option("delimiter",delimiter).csv(filePath) }
      else {
        sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").option("header",if(headerExist == true )  "true" else "false").csv(filePath)
      }
      case "json" => sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").json(filePath)
      case "parquet" => sparkSession.read.option("inferSchema",if(headerExist == true )  "true" else "false").parquet(filePath)
      case "text" => if(delimiter!="" || !delimiter.isEmpty || !delimiter.isBlank ) {
        sparkSession.read.option("inferSchema", if (headerExist == true) "true" else "false").option("header", if (headerExist == true) "true" else "false").option("delimiter",delimiter).text(filePath)
      }
      else{
        sparkSession.read.option("inferSchema", if (headerExist == true) "true" else "false").option("header", if (headerExist == true) "true" else "false").text(filePath)
      }
      case _ => throw new Exception()
    }
    fileInputDataFrame.show()
    return fileInputDataFrame;
  }

  def readDBToDataframe(dataTableBean: DataTableBean, sparkSession: SparkSession) : DataFrame ={

    val connectionProperties = new Properties()
    connectionProperties.put("user",dataTableBean.userName )
    connectionProperties.put("password",dataTableBean.password )

      val resultDataframe = sparkSession.read.jdbc(dataTableBean.url,dataTableBean.tableName,connectionProperties).where(dataTableBean.filterString)
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
