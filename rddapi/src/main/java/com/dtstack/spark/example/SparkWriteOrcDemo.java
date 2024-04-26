package com.dtstack.spark.example;

import com.dtstack.spark.example.function.MyWriteFunction;
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

public class SparkWriteOrcDemo {
    public static void main(String[] args) {
        String hdfsRoot=(String) args[0];
        SparkConf sparkConf = new SparkConf().setAppName("spark_orc_demo").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession spark_session = SparkSession
                .builder()
                .config(sparkConf)
                .appName("orcdemo")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        JavaRDD<String> stringJavaRDD = jsc.textFile("/Users/edy/spark.txt");
        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("name", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("age", DataTypes.IntegerType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);
        JavaRDD<Row> rowRdd = stringJavaRDD.map(new MyWriteFunction());
        String path=hdfsRoot + "/dtInsight/hive/warehouse/bigdata_test.db/test_orc5/";
        Dataset<Row> df = spark_session.createDataFrame(rowRdd, schema);
        df.write().format("orc").mode("append").orc(path);
        spark_session.close();
        jsc.close();
    }
}
