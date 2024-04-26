package com.dtstack.spark.demo

import com.dtstack.spark.demo.function.MyUdafFunction
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @Author xiaoyu
 * @Create 2021/12/22 17:07
 * @Description
 *
 */
object UdafDemoScala {
  def main(args: Array[String]): Unit = {
    // 设定spark计算框架的运行(部署) 环境
    val sparkConf: SparkConf = new SparkConf().setMaster("yarn").setAppName("spark udaf")

    // 创建SparkSql的环境对象
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val sparkudaf = new MyUdafFunction

    sparkSession.udf.register("sparkudaf",sparkudaf)

    import sparkSession.implicits._
    val dataFrame: DataFrame = List(("zs", 18, 100.0), ("zs", 18, 99.0), ("zs", 18, 98.0),("ls", 18, 97.0), ("ls", 18, 99.0), ("ls", 18, 98.0)).toDF("name", "age", "score")

    //给dataframe起一个表名
    dataFrame.createOrReplaceTempView("user")
    val result: DataFrame = sparkSession.sql("select name,sparkudaf(score) avgScore from user group by name")

    result.show()
    sparkSession.stop()
  }
}
