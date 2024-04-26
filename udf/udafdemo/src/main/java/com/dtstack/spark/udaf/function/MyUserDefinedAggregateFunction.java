package com.dtstack.spark.udaf.function;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * @Author longxuan
 * @Create 2021/10/11 09:58
 * @Description
 */

public class MyUserDefinedAggregateFunction extends UserDefinedAggregateFunction {
    private static final Logger LOG = LoggerFactory.getLogger(MyUserDefinedAggregateFunction.class);
    /**
     * initialize相当于初始化：
     * map端每个元素的初试值都为零
     * reduce端的每个元素的初始值都为零
     * update相当于map端的聚合
     * merge相当于reduce端的聚合
     *map端的 merge的好处：
     *1.减少了suffer磁盘的数据量
     *2.减少了reduce端拉取的数据量
     *3.减少了reduce端的聚合次数
     */

    /**
     * 指定输入字段的字段及类型
     *
     * @return
     */

    @Override
    public StructType inputSchema() {
        return DataTypes.createStructType(
                Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true)

                ));
    }

    /**
     * ** 在进行聚合造作的时候，所要处理的数据的结果的类型
     *
     * @return
     */
    @Override
    public StructType bufferSchema() {
        return DataTypes.createStructType(
                Arrays.asList(DataTypes.createStructField("one", DataTypes.IntegerType, true)));
    }

    /**
     * 指定UDAF函数计算后，返回的结果类型
     *
     * @return
     */
    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    /**
     * 确保一致性，一般用true 用以标记针对给定的一组输入 UDAF是否纵使生成相同的结果，
     *
     * @return
     */

    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * 初始化一个内部自定义的值 在Aggregate之前每组数据的初始化结果
     *
     * @param buffer 在map端每一个分区，中的每一个key做初始化，里边的值都为零
     *               initialize不仅作用在map端，初始化元素为零
     *               而且还作用在reduce端，初始化reduce端的每个元素的值也为零
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0);
    }

    /**
     * 更新，可以认为是一个一个地将组内的字段传递进来的，实现拼接的逻辑 buffer.getInt(0)获取的是上一次聚合后的值
     * 相当于map端的combiner,combiner就是对每一个map task的处理结果进行一次小的聚合 大聚合发生在reduce端
     * 这里即是：在进行聚合的时候，每当有新的值进来，对分组后得值如何进行计算
     *
     * @param buffer
     * @param arg1   update相当于map端的聚合 作用在每一个分区的每一个小组
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row arg1) {
        buffer.update(0, buffer.getInt(0) + 1);
    }

    /**
     * 合并update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据会在多个节点上处理
     * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来 buffer1.getInt(0):大聚合的时候，上一次聚合后的值
     * buffer2.getInt(0):这次计算传入进来的update的结果 这里即是：最后在分布式节点上完成传后需要进行全局级别的merge操作
     *
     * @param buffer1
     * @param buffer2 merge 作用在reduce端，将所有的数据拉取在一起
     *                reduce端跨分区，跨节点
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
    }

    /**
     * 最后返回一个和dataType方法的类型要一致的类型 返回UDAF最后的计算结果
     *
     * @param row
     * @return row是已经分好组的key
     */
    @Override
    public Object evaluate(Row row) {
        return row.getInt(0);
    }
}
