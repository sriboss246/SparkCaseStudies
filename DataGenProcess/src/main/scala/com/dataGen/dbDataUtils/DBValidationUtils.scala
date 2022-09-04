package com.dataGen.dbDataUtils

import org.apache.spark.sql.DataFrame

class DBValidationUtils {

     def columnCountValidation(df1 : DataFrame, df2 : DataFrame):Boolean = {
       val df1Columns = df1.columns
       val df2Columns = df2.columns

       if(df1Columns.size != df2Columns.size){
       return false;
      }
     return true;
   }

    def columnNameValidation(df1:DataFrame, df2:DataFrame):Boolean = {
      val df1Columns = df1.columns
      val df2Columns = df2.columns

      val columnsMatch = compareArrays(df1Columns,df2Columns)

      return columnsMatch
   }

  def rowCountValidation(df1:DataFrame,df2:DataFrame) : Boolean ={

    val df1Count = df1.count()
    val df2Count = df2.count()

    if(df1Count != df2Count)
      return false
    else
      return true
  }

  def compareArrays(arr1 : Array[String], arr2 : Array[String]) : Boolean = {
    val arr2List = arr2.toList
    for(arr <- arr1){

      if(!arr2List.contains(arr)){

        return false
      }
    }
    return true
  }



}
