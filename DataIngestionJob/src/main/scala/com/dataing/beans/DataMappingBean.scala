package com.dataing.beans

import scala.collection.mutable

case class DataMappingBean(sourceColumns: Array[String], columnMap:mutable.HashMap[String,String])
