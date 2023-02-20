package com.dataing.utils

import com.dataing.beans.DataMappingBean
import org.apache.spark.sql.DataFrame

trait DataIngestionStep {

    def processStep(dataset : DataFrame) : DataFrame
}
