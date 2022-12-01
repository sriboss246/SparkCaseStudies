package com.dataing.commons

import org.apache.log4j.Logger

trait LogHelper {

  lazy val log = Logger.getLogger(getClass.getName)

}
