package com.dataing.commons

import java.util

object AppConstants {

    val FILE_FORMAT_MAP = Map("CSV"->"csv", "PARQUET"->"parquet","JSON"->"json","XML"->"xml")
    val DATA_PREPARATION_STEPS_LIST: List[String] = List("remove_duplicate_rows","column_map","filter_null_by_col","fill_null_by_col")
    val DEBUG_FLAG = true

}
