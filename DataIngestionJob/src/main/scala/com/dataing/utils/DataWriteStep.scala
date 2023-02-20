package com.dataing.utils

import com.dataing.beans.{DataFileBean, DataTableBean, OutputBean}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class DataWriteStep {
  def writeDataToDataframe(outputBean: OutputBean, dataframe: DataFrame) : String = {

    val outputType = outputBean.outputType

    val result = outputType match {
      case "FILE" => {val dataFileBean = outputBean.fileBean
        writeFileToDataframe(dataFileBean,dataframe,outputBean.partitionColumns)}
      case "DB" =>  {val dataTableBean = outputBean.dataTableBean
        writeDBToDataframe(dataTableBean,dataframe,"")}
    }

    return "Success"

  }

def writeFileToDataframe(dataFileBean: DataFileBean, dataframe: DataFrame,partitionColumns:Array[String]): String ={

   if(partitionColumns.length>0){
     dataframe.write.partitionBy(partitionColumns:_*).format(dataFileBean.fileType).save(dataFileBean.location)
   }
  else{
     dataframe.write.format(dataFileBean.fileType).save(dataFileBean.location)
   }
 return "success"
}

def writeDBToDataframe(dataTableBean: DataTableBean, dataframe: DataFrame, mode:String) : String ={


    dataframe.write.mode("").jdbc("","",new Properties())

  return "success"
}

}
