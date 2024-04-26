package com.dtstack.spark.demo

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
 *
 * @Author xiaoyu
 * @Create 2021/12/16 17:50
 * @Description
 *
 */
object SparkTextFileDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("yarn").appName("Read TexFile").getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext
    val input: String = args(0)
    val output: String = args(1)
    //val input: String = "/Users/xuxiaoyu/Downloads/Hadoop/Source/textFile_source"
    //val output: String = "/Users/xuxiaoyu/Downloads/Hadoop/Sink/textFile"

    val rdd: RDD[String] = sparkContext.textFile(input,1)


    val result: RDD[(String, Int)] = rdd.flatMap(_.split(",")).map((_, 1)).groupBy(_._1).map(e => (e._1, e._2.size))


    //  获取文件系统
    val file_path = new org.apache.hadoop.fs.Path( output )
    val file_system = file_path.getFileSystem( sparkContext.hadoopConfiguration )

    //  判断路径存在时, 则删除
    if (file_system.exists( file_path )) {
      file_system.delete( file_path, true )
    }

    result.saveAsTextFile(output)

    sparkSession.stop()
  }
}
