package com.dataing.beans

case class DataPreparationStepBean(dataPreparationStepMap:java.util.Map[String,Boolean],filterNullCols : java.util.List[String],fillNullCols : java.util.Map[String,Any],dataMappingBean: DataMappingBean)
