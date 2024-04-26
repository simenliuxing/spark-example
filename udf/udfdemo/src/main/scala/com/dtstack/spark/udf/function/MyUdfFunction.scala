package com.dtstack.spark.udf.function

import org.apache.spark.sql.api.java.UDF1
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * @Author xiaoyu
 * @Create 2021/12/29 13:46
 * @Description
 *
 */
class MyUdfFunction extends UDF1[String,String]{
  private val LOG:Logger = LoggerFactory.getLogger(this.getClass)
  override def call(t1: String): String = {
    var s:String = ""
    try {
      if (!"".equals(t1) || t1 != null) {
        s = t1 + "dtstack"
      }
    } catch {
      case e:Exception => LOG.error(e.toString)
    }
    s
  }
}
