package com.dataing.beans

case class DataFileBean(fileName:String, fileType:String, sourceType:String, location:String, headerExist:Boolean, schema:Map[String,String])
