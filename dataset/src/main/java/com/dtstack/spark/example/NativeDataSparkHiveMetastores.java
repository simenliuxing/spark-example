package com.dtstack.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NativeDataSparkHiveMetastores {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hive_meta_demo");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession spark_session = SparkSession
                .builder()
                .config(sparkConf)
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();
        spark_session.conf().set("spark.sql.crossJoin.enabled", true);
        spark_session.conf().set("spark.sql.hive.convertMetastoreParquet","false");
        spark_session.conf().set("spark.sql.hive.convertMetastoreOrc","false");
        //mysql的jdbc
        String url = "jdbc:mysql://ip:port/database?characterEncoding=utf-8&useSSL=false";
        //库名通过args参数传播
        String target_db = String.valueOf(args[0]);
        Properties prop = new Properties();
        prop.setProperty("user", "*******");
        prop.setProperty("password", "********");
        prop.setProperty("driver", "com.mysql.jdbc.Driver");
        Dataset<Row> df1 = spark_session.read().jdbc(url, "TBLS", prop);
        Dataset<Row> df2 = spark_session.read().jdbc(url, "DBS", prop);
        Dataset<Row> df3 = spark_session.read().jdbc(url, "TABLE_PARAMS", prop);
        Dataset<Row> df4 = spark_session.read().jdbc(url, "SDS", prop);
        Dataset<Row> df5 = spark_session.read().jdbc(url, "COLUMNS_V2", prop);
        df1.createOrReplaceTempView("TBLS");
        df2.createOrReplaceTempView("DBS");
        df3.createOrReplaceTempView("TABLE_PARAMS");
        df4.createOrReplaceTempView("SDS");
        df5.createOrReplaceTempView("COLUMNS_V2");


        //1.根据库名从mysql查询表，得到表的拥有者、数据库名、表名、列名、字段类型
        String target_table = "select" +
                "  t2.NAME database_name," +
                "  UPPER(t1.TBL_NAME) as table_name, " +
                "  first(t3.PARAM_VALUE) table_comment, " +//表注释
                "  UPPER(t5.COLUMN_NAME) as column_name, " +
                "  first(t5.COMMENT) column_comment, " +//列注释
                "  first(t5.INTEGER_IDX) integer_idx " +//列排名
                " FROM " +
                "  tbls t1 " +
                " JOIN " +
                "  dbs t2 " +
                " ON " +
                "  t1.DB_ID = t2.DB_ID " +
                " JOIN " +
                "  table_params t3 " +
                " ON " +
                "  t1.TBL_ID = t3.TBL_ID " +
                " JOIN " +
                "  sds t4 " +
                " ON " +
                "  t1.SD_ID = t4.SD_ID " +
                " JOIN " +
                "  columns_v2 t5 " +
                " ON " +
                "  t4.CD_ID = t5.CD_ID " +
                " where t2.NAME='" + target_db + "' and t1.TBL_NAME not like 'select_sql_temp_table%' and t1.TBL_NAME not like 'temp_%'" +
                " group by t2.NAME, t1.TBL_NAME, t5.COLUMN_NAME " +
                " order by t1.TBL_NAME,first(t5.INTEGER_IDX)";

        spark_session.sql(target_table).createOrReplaceTempView("mysql_data");
        ArrayList<String> table_num = new ArrayList<String>();
        table_num.add(String.valueOf(args[1]));//表名



        //建立mysql和hive的最终结果表 有数据库名、表名、列名、字段排序、表的总数、非空值、饱和度
        spark_session.sql("drop table if exists datawarehouse.result_table_information");
        spark_session.sql("create table datawarehouse.result_table_information(database_name STRING,table_name STRING,table_comment STRING,column_name STRING,column_comment STRING,field_sort BIGINT,total_num BIGINT,unempty_num BIGINT) stored as orc");

        String pinjie_sql = "";
        String target_tbl = "";

        //遍历这个库名的所有表
        for (int j = 0; j < table_num.size(); j++) {
            if (j == 0) {//第一个表
                pinjie_sql += " (";
            }
            target_tbl = table_num.get(j);
            // 获取这个表的值

            List<Integer> total_num = spark_session.sql("SELECT count(*) as total_num " + "from " + target_db + "." + target_tbl).select("total_num").toJavaRDD()
                    .map(new Function<Row, Integer>() {
                        @Override
                        public Integer call(Row row) throws Exception {
                            Long o = row.getLong(0);
                            return o.intValue();
                        }
                    }).collect();
            int table_total = total_num.get(0);
            ArrayList<String> field_list = new ArrayList<String>();

            // 根据mysql查询的表名去desc hive的列
            List<Row> collect = spark_session.sql("desc " + target_db + "." + target_tbl).toJavaRDD().collect();
            for (int k = 0; k < collect.size(); k++) {
                String line = collect.get(k).getString(0);
                if ("pt".equals(line)) {//下边的不计算
                    continue;
                }
                if (line.startsWith("#")) {
                    break;
                } else {
                    field_list.add(line);//自动去重
                }
            }


            for (int i = 0; i < field_list.size(); i++) {
                //单个分片的只计算空值，最后在累加计算总值
                if (field_list.size() == 1 && i == 0 && table_num.size() == 1 && j == 0) {//如果只有一张表 一个字段,且都是第一个，结束循环，外边的for循环不会继续
                    pinjie_sql += "  select UPPER(table_name) as table_name,UPPER(column_name) as column_name, " + table_total + " as total_num, field_cnt as unempty_num  from (select '" + target_tbl + "' as table_name, '" + field_list.get(i) + "' as column_name, sum(case when " + field_list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp ) ";
                    break;
                } else if (field_list.size() == 1 && i == 0 && j == table_num.size() - 1 && table_num.size() > 1) {//不止一个表，这个表只有一个字段,如果这是最后一个表的第一个字段，也是最后一张表的自后一个字段，结束这个循环 就不会再去遍历外边的for循环
                    pinjie_sql += "  select UPPER(table_name) as table_name,UPPER(column_name) as column_name, " + table_total + " as total_num, field_cnt as unempty_num  from (select '" + target_tbl + "' as table_name, '" + field_list.get(i) + "' as column_name, sum(case when " + field_list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp )";
                    break;
                } else if (field_list.size() > 1 && i == field_list.size() - 1 && j == table_num.size() - 1 && table_num.size() > 1) {//不止一个表，这是最后一个表，这个表不止一个字段，如果是这个表的最后一个字段， 那就去掉union all
                    pinjie_sql += " select UPPER(table_name) as table_name,UPPER(column_name) as column_name , " + table_total + "  as total_num , field_cnt as unempty_num  from (select '" + target_tbl + "' as table_name, '" + field_list.get(i) + "' as column_name,sum(case when " + field_list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp )";
                    break;
                }                //不止一张表，不是最后一个表的最后一个字段，且不是最后一个表的第一个字段，且这个表不是只有一个字段 比如4张表 第一张表有两个字段第一个字段
                else if (field_list.size() > 1 && i > 0 && j != table_num.size() - 1 && table_num.size() > 1) {//不止一个表，这个表不止一个字段,如果这不是最后一个表的第一个字段，也要加上union all去关联其他字段其他表 然后结束循环继续下一个表
                    pinjie_sql += "  select UPPER(table_name) as table_name,UPPER(column_name) as column_name, " + table_total + " as total_num, field_cnt as unempty_num  from (select '" + target_tbl + "' as table_name, '" + field_list.get(i) + "' as column_name, sum(case when " + field_list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp union all ";
                    //只有1张表 如果是这个表的最后一个字段
                } else if (i == field_list.size() - 1 && table_num.size() == 1) {
                    pinjie_sql += " select UPPER(table_name) as table_name,UPPER(column_name) as column_name, " + table_total + " as total_num , field_cnt as unempty_num  from (select '" + target_tbl + "' as table_name, '" + field_list.get(i) + "' as column_name, sum(case when " + field_list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp  ) ";
                }else {
                    pinjie_sql += " select UPPER(table_name) as table_name,UPPER(column_name) as column_name, " + table_total + " as total_num , field_cnt as unempty_num  from (select '" + target_tbl + "' as table_name, '" + field_list.get(i) + "' as column_name, sum(case when " + field_list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp  union all ";
                }
            }
            if (j == table_num.size() - 1) {//如果是最后一个表
                spark_session.sql("insert into datawarehouse.result_table_information  " +
                        "select mysql_data.database_name,hive_result_table.table_name,first(mysql_data.table_comment) as table_comment,hive_result_table.column_name,first(mysql_data.column_comment) as column_comment,first(mysql_data.integer_idx) as field_sort,first(hive_result_table.total_num ) as total_num,if(sum(hive_result_table.unempty_num) is null or sum(hive_result_table.unempty_num)=0,0,sum(hive_result_table.unempty_num)) as  unempty_num " +
                        " from mysql_data  inner join  " + pinjie_sql +
                        "  as  hive_result_table    on  mysql_data.table_name=hive_result_table.table_name and mysql_data.column_name=hive_result_table.column_name  " +
                        " group by mysql_data.database_name,hive_result_table.table_name,hive_result_table.column_name");
            }
        }

        spark_session.sql("drop table if exists datawarehouse.result_hive_meta_info ");
        spark_session.sql("create table datawarehouse.result_hive_meta_info(database_name STRING,table_name STRING,table_comment STRING,column_name STRING,column_comment STRING,field_sort BIGINT,total_num BIGINT,unempty_num BIGINT,saturation STRING) stored as orc");
        spark_session.sql("insert into datawarehouse.result_hive_meta_info " +
                "select database_name, table_name, table_comment,column_name, column_comment,field_sort, total_num,unempty_num, saturation from ( select  database_name, table_name, table_comment,  column_name,column_comment, field_sort, total_num, unempty_num,if(total_num-unempty_num=0,'100.00%',CONCAT(cast(((unempty_num/total_num)*100) AS STRING),'%')) as saturation from datawarehouse.result_table_information )  as result_tmp distribute by table_name sort by table_name asc,field_sort asc");
        spark_session.close();
        jsc.close();
    }
}
