package com.dtstack.spark.example;
import java.io.IOException;
import java.net.URI;

import com.dtstack.spark.example.function.MyFunction;
import com.dtstack.spark.example.function.MyFunction2;
import com.dtstack.spark.example.function.MyPairFunction;
import com.dtstack.spark.example.function.MyVoidFunction;
import com.dtstack.spark.example.utils.TestPath;
import com.dtstack.spark.example.utils.TestUrl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @Author longxuan
 * @Create 2021/11/11 09:58
 * @Description
 */
public class SparkReadOrcDemo {
    public static void main(String[] args) throws IOException {
        String hdfsRoot=(String) args[0];
        SparkConf sparkConf = new SparkConf().setAppName("spark_orc_demo").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession spark_session = SparkSession
                .builder()
                .config(sparkConf)
                .appName("orcdemo")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
//        //读取本地parquet文件
//        Dataset<Row> orcsession = spark_session.read().orc("/Users/edy/test_orc/");
//        //转换成rdd
//        JavaRDD<Row> rowJavaRDD = orcsession.toJavaRDD();
//        //读取单列的值，做成WordCOunt的效果
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = rowJavaRDD.map(new MyFunction()).mapToPair(new MyPairFunction()).reduceByKey(new MyFunction2());
//        stringIntegerJavaPairRDD.foreach(new MyVoidFunction());

//        //读取hdfs上的文件
        URI fsUri=new TestUrl().getUrl(hdfsRoot);
        FileSystem fs = FileSystem.get(fsUri, new Configuration());
        String path=hdfsRoot + "/dtInsight/hive/warehouse/bigdata_test.db/test_orc5/";
        if(fs.exists(new TestPath().getPath(path))){
            //如果存在就读取单列的值，做成WordCOunt的效果
            Dataset<Row> orcData = spark_session.read().orc(path);
            JavaRDD<Row> rowJavaRDD = orcData.toJavaRDD();
            JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = rowJavaRDD.map(new MyFunction()).mapToPair(new MyPairFunction()).reduceByKey(new MyFunction2());
            stringIntegerJavaPairRDD.foreach(new MyVoidFunction());
        }
        spark_session.close();
        jsc.close();

    }
}
