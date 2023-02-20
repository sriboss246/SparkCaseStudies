package com.dataing.beans

case class DataPreparationStepBean(dataPreparationStepMap:Map[String,Boolean],filterNullCols : List[String],fillNullCols : Map[String,Any],dataMappingBean: DataMappingBean)
