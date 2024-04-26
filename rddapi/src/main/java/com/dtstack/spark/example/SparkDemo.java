package com.dtstack.spark.example;

import com.dtstack.spark.example.function.MyFlatMapFunction;
import com.dtstack.spark.example.function.MyFunction2;
import com.dtstack.spark.example.function.MyPairFunction;
import com.dtstack.spark.example.function.MyVoidFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
@SuppressWarnings("AlibabaRemoveCommentedCode")
public class SparkDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("spark_demo").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<String> data = Arrays.asList("hello", "world", "hello", "next");
        //  JavaRDD<String> rdd01 = jsc.textFile("/Users/edy/spark.txt");
        JavaRDD<String> rdd01 = jsc.parallelize(data);
        JavaPairRDD<String, Integer> resRdd = rdd01.flatMap(new MyFlatMapFunction())
                .mapToPair(new MyPairFunction()).reduceByKey(new MyFunction2());
        resRdd.foreach(new MyVoidFunction());
        jsc.stop();
    }
}
