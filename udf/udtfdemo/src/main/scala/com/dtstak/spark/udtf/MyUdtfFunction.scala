package com.dtstak.spark.udtf

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.slf4j.{Logger, LoggerFactory}

import java.util
/**
 *
 * @Author xiaoyu
 * @Create 2021/12/29 12:03
 * @Description
 *
 */
class MyUdtfFunction extends GenericUDTF{
  private val LOG:Logger = LoggerFactory.getLogger(this.getClass)
  //创建输出集合，因为可能炸出多列，所以用集合存储
  private val outList = new util.ArrayList[String]

  /**
   * 当前函数的作用：规定炸裂之后输出的列的别名以及炸裂之后形成的列中存储的数据类型
   */
  @throws[UDFArgumentException]
  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = { //1.定义输出数据的列名和类型
    //用来存储炸裂之后的列的列名，因为炸裂函数可能炸出多列，所以在存储列名的时候需要一个集合
    val fieldNames = new util.ArrayList[String]
    // 用于存储炸裂之后，输出列的类型
    val fieldOIs = new util.ArrayList[ObjectInspector]
    //2.添加输出数据的列名和类型
    fieldNames.add("lineToWord")
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }
  /**
   * 真正的逻辑，读取输入数据，处理数据，返回结果
   * 在使用函数的时候，传入的实参会存储到process方法对应参数的Object数组中
   */

  override def process(args: Array[AnyRef]): Unit = {
    try {
      //1.获取原始数据
      val arg: String = args(0).toString
      //2.获取数据传入的第二个参数，此处为分隔符
      val splitKey: String = args(1).toString
      //3.将原始数据按照传入的分隔符进行切分
      val fields: Array[String] = arg.split(splitKey)
      //4.遍历切分后的结果，并写出
      for (field <- fields) { //集合为复用的，首先清空集合
        outList.clear
        //将每一个单词添加到集合
        outList.add(field)
        //将集合内容写出
        forward(outList)
      }
    } catch {
      case e: Exception =>
        LOG.error(e.toString)
    }
  }

  override def close(): Unit = {

  }

}
