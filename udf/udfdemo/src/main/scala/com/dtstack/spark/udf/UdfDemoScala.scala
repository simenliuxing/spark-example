package com.dtstack.spark.udf

import com.dtstack.spark.udf.function.MyUdfFunction
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @Author xiaoyu
 * @Create 2021/12/22 16:48
 * @Description
 *
 */
object UdfDemoScala {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("yarn").setAppName("spark_udf")

    //创建sparksql的环境对象
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    val dataFrame: DataFrame = List(("zs", 18, 100.0), ("ls", 19, 99.0), ("ww", 20, 98.0)).toDF("name", "age", "score")

    //向spark中注册一个udf函数
    val sparkudf = new MyUdfFunction
    sparkSession.udf.register("sparkudf",sparkudf,StringType)

    //给dataframe起一个表名
    dataFrame.createOrReplaceTempView("user")
    val result: DataFrame = sparkSession.sql("select sparkudf(name) name,age,score from user")

    result.show()

    sparkSession.stop()
  }

}
