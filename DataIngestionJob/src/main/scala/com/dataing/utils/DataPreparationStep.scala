package com.dataing.utils
import com.dataing.beans.{DataMappingBean, DataPreparationStepBean}
import com.dataing.commons.AppConstants
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import scala.collection.JavaConverters._

import scala.collection.mutable

class DataPreparationStep(dataPreparationStepBean: DataPreparationStepBean) extends DataIngestionStep
{
  override def processStep(dataset: DataFrame): DataFrame = {

    val dataPreparationStepMap = dataPreparationStepBean.dataPreparationStepMap
    val dataPreparationStepList = AppConstants.DATA_PREPARATION_STEPS_LIST
    var resultDataframe : DataFrame = dataset
    val filterNullCols = dataPreparationStepBean.filterNullCols
    val fillNullCols = dataPreparationStepBean.fillNullCols
    val debugFlag = AppConstants.DEBUG_FLAG

    for( step <- dataPreparationStepList){

         if(dataPreparationStepMap.get(step).toString.toUpperCase().equals("TRUE")){

          resultDataframe = step match {
            case "remove_duplicate_rows" => removeDuplicateRows(resultDataframe)
            case "column_map" => mapColumns(resultDataframe,dataPreparationStepBean.dataMappingBean.columnMap.asScala.toMap)
            case "filter_null_by_col" => filterNullByColumn(resultDataframe,filterNullCols.asScala.toList)
            case "fill_null_by_col" => fillNullByColumn(resultDataframe,fillNullCols.asScala.toMap)
          }

           if(debugFlag){
             resultDataframe.show()
           }
           return resultDataframe
         }
    }
    return dataset
  }
def fillNullByColumn(inputDataframe : DataFrame,nullColumnMap : Map[String,Any]):DataFrame = {

       val resultDataframe =  inputDataframe.na.fill(nullColumnMap)
       return resultDataframe
}

def filterNullByColumn(inputDataframe : DataFrame,nullColumnList : List[String]) : DataFrame = {

  val resultDataframe = inputDataframe.na.drop(nullColumnList)
  return resultDataframe
}

def mapColumns(inputDataframe : DataFrame, columnsMap : Map[String,String]) : DataFrame = {

  val columns = inputDataframe.columns
  var resultDataframe : DataFrame = null
  for(column <- columns){
    if(null!= columnsMap.get(column)){

      resultDataframe = inputDataframe.withColumnRenamed(column,columnsMap.get(column).toString)
    }

  }
   return resultDataframe
}

def removeDuplicateRows(inputDataframe : DataFrame) : DataFrame = {

    val resultDataframe = inputDataframe.distinct()
    return resultDataframe
}

}
