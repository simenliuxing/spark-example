package com.dtstack.spark.udaf;

import com.dtstack.spark.udaf.function.MyUserDefinedAggregateFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * @Author longxuan
 * @Create 2021/10/11 09:58
 * @Description
 */
public class UdafDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL UDAF basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu", "zhangsan", "zhangsan", "lisi", "wangwu");
        JavaRDD<String> parallelize = jsc.parallelize(list);
        JavaRDD<Row> RowRdd = parallelize.map(new Function<String, Row>() {

            /**
             * map是一对一的类型
             * 进去的是String类型，出来的是row类型
             */
            @Override
            public Row call(String s) throws Exception {
                return RowFactory.create(s);
            }
        });
        List<StructField> fields = new ArrayList<StructField>();
        /**
         * 创建名为name的区域，类型为String
         */
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        /**
         * 创建schema
         */
        StructType schema = DataTypes.createStructType(fields);
        /**
         * 将rdd和schema相聚和
         */
        Dataset<Row> dataFrame = spark.createDataFrame(RowRdd, schema);

        /**
         * 创建一个用户名为user的表格
         */
        dataFrame.registerTempTable("user");
        spark.udf().register("StringCount",new MyUserDefinedAggregateFunction());
        spark.sql("select name,StringCount(name) as strCount from user group by name").show();
        jsc.stop();
        spark.stop();
    }
}
