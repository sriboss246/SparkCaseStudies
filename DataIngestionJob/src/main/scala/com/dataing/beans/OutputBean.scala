package com.dataing.beans

case class OutputBean(fileBean:DataFileBean,dataTableBean: DataTableBean,partitionColumns:Array[String],outputType:String)
