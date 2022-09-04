package com.dataGen.dbDataUtils

import org.apache.spark.sql.DataFrame

import java.util.Properties

class DBDataTransferUtils {

  def dbDataWriter(dbDataframe : DataFrame, dbProps : Properties): Long = {

    var dest_props : Properties = null
    dest_props.setProperty("user_name",dbProps.get("dest_user_name").toString)
    dest_props.setProperty("password",dbProps.get("dest_password").toString)
    val rowCount = dbDataframe.write.jdbc(dbProps.get("dest_url").toString,dbProps.get("dest_table_name").toString,dest_props)

        return rowCount;
  }

}
