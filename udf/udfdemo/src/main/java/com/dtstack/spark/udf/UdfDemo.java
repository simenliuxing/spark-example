package com.dtstack.spark.udf;

import com.dtstack.spark.udf.function.MyFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SparkSession;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class UdfDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("spark_demo").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        //json数据 {"name":"dtstack","age":12,"timestamp":"2021-08-08 10:10:10"}
        Dataset<Row> df = spark.read().json("/Users/edy/spark.json");

        df.createOrReplaceTempView("people");

        //1.第一种方法
        //spark.udf().register("toup", (String line) -> line.toUpperCase(), DataTypes.StringType);
        //2.第二种方法
        spark.udf().register("toup",new MyFunction(),DataTypes.StringType);
        Dataset<Row> SqlDf = spark.sql("select toup(name) from people");
        SqlDf.show();
        jsc.stop();
        spark.stop();
    }
}


