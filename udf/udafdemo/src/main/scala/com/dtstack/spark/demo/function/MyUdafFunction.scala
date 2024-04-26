package com.dtstack.spark.demo.function

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
 *
 * @Author xiaoyu
 * @Create 2021/12/22 17:56
 * @Description
 *
 */
class MyUdafFunction extends UserDefinedAggregateFunction{

    //输入的数据类型
    override def inputSchema: StructType = {
      StructType(List(StructField("value",DoubleType)))
    }

    //产生中间结果的数据类型
    //相当于每个分区里都要进行计算
    override def bufferSchema: StructType = {
      StructType(List(StructField("score",DoubleType),StructField("count",IntegerType)))
    }

    //最终返回的结果类型
    override def dataType: DataType = DoubleType

    //对于相同的输入是否一直返回相同的输出
    override def deterministic: Boolean = true

    //初始化，每个分区都要有初始值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0.0
      buffer(1) = 0
    }

    //将缓冲区的数据加上输入的数据，然后更新到缓冲区
    //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //每有一个数字参与运算就进行相加(包含了之前的中间结果)
      buffer(0) = buffer.getDouble(0)+input.getDouble(0)
      //参与运算数据的个数
      buffer(1) = buffer.getInt(1)+1

    }

    //全局聚合，每个分区进行聚合计算
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //每个分区计算的结果进行汇总
      buffer1(0) = buffer1.getDouble(0)+buffer2.getDouble(0)
      buffer1(1) = buffer1.getInt(1)+buffer2.getInt(1)
    }

    //返回最终结果
    override def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)/buffer.getInt(1)
    }
}
