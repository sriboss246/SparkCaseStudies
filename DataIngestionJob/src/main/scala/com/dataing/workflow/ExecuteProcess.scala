package com.dataing.workflow

import com.dataing.beans.{DataIOBean, DataPreparationStepBean}
import com.dataing.commons.LogHelper
import com.dataing.utils.{DataIngestionStep, DataPreparationStep, DataReadStep, DataWriteStep}
import com.google.gson.Gson
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.mapAsScalaMap
import scala.collection.Map

object ExecuteProcess extends App with LogHelper{

  execute(args)
override  def main(args : Array[String]): Unit =super.main(args)

  def execute(args:Array[String]) : Integer ={
       log.info("DataIngestion App Input Arguments ===> "+args.toList)
       val gson = new Gson
       try{

         val envMappingJson : String = args(0)
         val dataIOJson : String = args(1)
         val taskJson : String = args(2)



          val propsMap:Map[String,String] = readPropsFile()

          val envMappingMap = gson.fromJson(envMappingJson,classOf[Map[String,String]]).toMap[String,String]
          val dataIOBean = gson.fromJson(dataIOJson,classOf[DataIOBean])
          val taskBean = gson.fromJson(taskJson,classOf[DataPreparationStepBean])

          val sparkInit = new SparkInit
          val spark = sparkInit.getSparkSession(envMappingMap)

         val dataRead = new DataReadStep()
         val dataWrite = new DataWriteStep()
         val dataProcess = new DataPreparationStep(taskBean)
         val inputDataframe = dataRead.readDataToDataframe(dataIOBean.inputBean,spark)
         val interimProcessedDataframe = dataProcess.processStep(inputDataframe)
         dataWrite.writeDataToDataframe(dataIOBean.outputBean,interimProcessedDataframe)

       }

       catch {
          case e : Exception => { throw  new Exception("Error executing Spark job",e)}
        }
      1
    }

  def readPropsFile() : Map[String,String] = {

     return null
  }

}
