package com.dataing.workflow

import com.dataing.commons.LogHelper
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
          val integrationJson : String = args(0)
          val fileUploadJson : String = args(1)
          val envMappingJson : String = args(2)

          val propsMap:Map[String,String] = readPropsFile()

          val envMappingMap = gson.fromJson(envMappingJson,classOf[Map[String,String]]).toMap[String,String]

          val integrationName : String = envMappingMap("")

          val conf : SparkConf = new SparkConf()
          envMappingMap.keys.map(key => conf.set(key,envMappingMap(key)))
          propsMap.keys.map(key => conf.set(key,propsMap(key)))

          val spark = SparkSession.builder().config(conf).appName("").enableHiveSupport().getOrCreate()

          integrationName match {
             case "" =>
             case _ =>
           }

       }

       catch {
          case e : Exception => { throw  new Exception("Error executing Spark job",e)}
        }
      1
    }

  def readPropsFile() : Map[String,String] = {


  }

}
