package com.dataing.beans

case class DataFileBean(fileName:String, fileType:String, sourceType:String, location:String, headerExist:Boolean,delimiter:String,schema:java.util.Map[String,String])
