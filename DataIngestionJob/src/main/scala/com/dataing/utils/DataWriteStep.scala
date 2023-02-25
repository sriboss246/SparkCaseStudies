package com.dataing.utils

import com.dataing.beans.{DataFileBean, DataTableBean, OutputBean}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

class DataWriteStep {
  def writeDataToDataframe(outputBean: OutputBean, dataframe: DataFrame) : String = {

    val outputType = outputBean.outputType

    val result = outputType match {
      case "FILE" => {val dataFileBean = outputBean.fileBean
        writeDataframeToFile(dataFileBean,dataframe,outputBean.partitionColumns)}
      case "DB" =>  {val dataTableBean = outputBean.dataTableBean
        writeDataframeToDB(dataTableBean,dataframe,"")}
    }

    return "Success"

  }

def writeDataframeToFile(dataFileBean: DataFileBean, dataframe: DataFrame,partitionColumns:Array[String]): String ={

   if(partitionColumns.length>0){
     dataframe.write.partitionBy(partitionColumns:_*).format(dataFileBean.fileType).save(dataFileBean.location)
   }
  else{
     dataframe.write.format(dataFileBean.fileType).save(dataFileBean.location)
   }
 return "success"
}

def writeDataframeToDB(dataTableBean: DataTableBean, dataframe: DataFrame, mode:String) : String ={

  val connectionProperties = new Properties()
  connectionProperties.put("user",dataTableBean.userName )
  connectionProperties.put("password",dataTableBean.password )
  dataframe.show()
    dataframe.write.mode(dataTableBean.mode).jdbc(dataTableBean.url,dataTableBean.tableName,connectionProperties)

  return "success"
}

}
