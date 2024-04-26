package com.dtstack.spark.demo


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * @Author xiaoyu
 * @Create 2021/12/16 11:30
 * @Description
 *
 */
object SparkOrcDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("yarn").appName("Read Orc").getOrCreate()

    val input: String = args(0)
    val output: String = args(1)
    val dataFrame: DataFrame = sparkSession.read.orc(input)
    //dataFrame.show()

    val rdd:RDD[Row] = dataFrame.rdd

    val value: RDD[(String, Int)] = rdd.map(e => {
      e.get(0).toString
    }).flatMap(_.split(",")).map((_, 1)).groupBy(_._1).map(e => (e._1, e._2.size))

    val result: RDD[Row] = value.map(e => {
      Row(e._1, e._2)
    })

    //关联schema(字段名称，数据类型，是否可以为空)
    val structType: StructType = StructType(
      Array(
        StructField("word", StringType),
        StructField("count", IntegerType)
      )
    )

    val frame: DataFrame = sparkSession.createDataFrame(result, structType)


    //  获取文件系统
    val file_path = new org.apache.hadoop.fs.Path( output )
    val file_system = file_path.getFileSystem( sparkSession.sparkContext.hadoopConfiguration )

    //  判断路径存在时, 则删除
    if (file_system.exists( file_path )) {
      file_system.delete( file_path, true )
    }

    frame.write.orc(output)

    sparkSession.stop()
  }

}
