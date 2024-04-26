package com.dtstack.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class DatabaseSparkHiveMetastores {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hive_demo").setMaster("local[*]").set("spark.driver.host", "localhost");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession spark_session = SparkSession
                .builder()
                .config(sparkConf)
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .enableHiveSupport()
                .getOrCreate();
        spark_session.conf().set("spark.sql.crossJoin.enabled", true);
        //Catalog catalog1 = spark_session.catalog();

        //mysql的jdbc
        String url = "jdbc:mysql://ip:port/database?characterEncoding=utf-8&useSSL=false";
        //库名通过args参数传播
        String target_db = String.valueOf(args[0]);
        Properties prop = new Properties();
        prop.setProperty("user", "******");
        prop.setProperty("password", "**********");

        //在driver端读取
        Dataset<Row> df1 = spark_session.read().jdbc(url, "tbls", prop);
        Dataset<Row> df2 = spark_session.read().jdbc(url, "dbs", prop);
        Dataset<Row> df3 = spark_session.read().jdbc(url, "table_params", prop);
        Dataset<Row> df4 = spark_session.read().jdbc(url, "sds", prop);
        Dataset<Row> df5 = spark_session.read().jdbc(url, "columns_v2", prop);
        df1.createOrReplaceTempView("tbls");
        df2.createOrReplaceTempView("dbs");
        df3.createOrReplaceTempView("table_params");
        df4.createOrReplaceTempView("sds");
        df5.createOrReplaceTempView("columns_v2");


        //1.根据库名从mysql查询表，得到表的拥有者、数据库名、表名、列名、字段类型

        String target_table = "select" +
                "  t2.NAME database_name," +
                "  t1.TBL_NAME table_name, " +
                "  first(t3.PARAM_VALUE) table_comment, " +
                "  t5.COLUMN_NAME column_name, " +
                "  first(t5.COMMENT) column_comment, " +
                "  first(t5.INTEGER_IDX) integer_idx " +
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


        //把mysql查出来的表名筛选
        Dataset<Row> sql = spark_session.sql("select distinct(table_name) as table_name from mysql_data");
        Iterator<Row> target_table_name = sql.toLocalIterator();
        ArrayList<String> table_num = new ArrayList<String>();

        //得到目标表集合
        while (target_table_name.hasNext()) {
            Row next = target_table_name.next();
            String name = (String) next.get(0);
            table_num.add(name.toLowerCase());
        }
        //建立mysql和hive的最终结果表 有数据库名、表名、列名、字段排序、表的总数、空值、饱和度
        spark_session.sql("drop table if exists default.result_table_information");
        spark_session.sql("create table default.result_table_information(database_name STRING,table_name STRING,table_comment STRING,column_name STRING,column_comment STRING,field_sort BIGINT,total_num BIGINT,empty_num BIGINT,saturation STRING)");

        String pinjie_sql = "";
        String target_tbl = "";

        //遍历这个库名的所有表
        for (int j = 0; j < table_num.size(); j++) {
            if (j == 0) {//第一个表
                pinjie_sql += "(";
            }
            target_tbl = table_num.get(j);
            spark_session.sql("CACHE LAZY TABLE " + target_db + "." + target_tbl);
            //根据mysql查询的表名去desc hive的列
            Iterator<Row> rowIterator = spark_session.sql("desc " + target_db + "." + target_tbl).toLocalIterator();
            Set<String> set = new HashSet<String>();
            while (rowIterator.hasNext()) {
                String line = (String) rowIterator.next().get(0);//取出字段名
                if (line.startsWith("#")) {//去掉#开头
                    continue;
                }
                set.add(line);//自动去重
            }
            ArrayList<String> list = new ArrayList<String>(set);
//          遍历一个表所有的列
            for (int i = 0; i < list.size(); i++) {
                if (list.size() == 1 && i == 0 && table_num.size() == 1 && j == 0) {//如果只有一张表 一个字段,且都是第一个，结束循环，外边的for循环不会继续
                    pinjie_sql += "  select table_name,field_name, table_total as total_num, field_cnt as empty_num , if((table_total - field_cnt)/table_total is null,'100.00%',CONCAT(CAST(round(((table_total - field_cnt)/table_total)*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '" + list.get(i) + "' as field_name,count(*) as table_total, count(case when " + list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp ) ";
                    break;
                } else if (list.size() == 1 && i == 0 && j == table_num.size() - 1) {//不止一个表，只有一个字段,如果这是最后一个表的第一个字段，也是最后一张表的自后一个字段，结束这个循环 就不会再去遍历外边的for循环
                    pinjie_sql += "  select table_name,field_name, table_total as total_num, field_cnt as empty_num , if((table_total - field_cnt)/table_total is null,'100.00%',CONCAT(CAST(round(((table_total - field_cnt)/table_total)*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '" + list.get(i) + "' as field_name,count(*) as table_total, count(case when " + list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp )";
                    //pinjie_sql+=" ( select table_name,field_name, "+table_total+" as total_num, field_cnt as empty_num,if(("+table_total+" - field_cnt)/"+table_total+" is null,'100.00%',CONCAT(CAST(round((("+table_total+" - field_cnt)/"+table_total+")*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '"+list.get(i)+"' as field_name,count(case when "+list.get(i)+" is not null then 1 else 0 end) as field_cnt from "+ target_db + "." + target_tbl+" ) as "+target_tbl+String.valueOf(i)+"tmp ) ";
                    break;
                } else if (i == list.size() - 1 && j == table_num.size() - 1) {//不止一个表，每张表不止一个字段，如果是这个表的最后一个字段，且这是最后一个表 那就去掉union all
                    pinjie_sql += " select table_name,field_name , table_total  as total_num , field_cnt as empty_num , if((table_total  - field_cnt)/ table_total  is null,'100.00%',CONCAT(CAST(round((( table_total  - field_cnt)/ table_total )*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '" + list.get(i) + "' as field_name,count(*) as table_total ,count(case when " + list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp )";
                    // pinjie_sql+="  select table_name,field_name, "+table_total+" as total_num, field_cnt as empty_num,if(("+table_total+" - field_cnt)/"+table_total+" is null,'100.00%',CONCAT(CAST(round((("+table_total+" - field_cnt)/"+table_total+")*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '"+list.get(i)+"' as field_name,count(case when "+list.get(i)+" is not null then 1 else 0 end) as field_cnt from "+ target_db + "." + target_tbl+" ) as "+target_tbl+String.valueOf(i)+"tmp )";
                    break;
                }
                //不止一张表，不是最后一个表的最后一个字段，且不是最后一个表的第一个字段，且这个表只有一个字段 下标就是0
                else if (list.size() == 1 && i == 0 && j != table_num.size() - 1) {//不止一个表，不止一个字段,如果这不是最后一个表的第一个字段，也要加上union all去关联其他字段其他表 然后结束循环继续下一个表
                    pinjie_sql += "  select table_name,field_name, table_total as total_num, field_cnt as empty_num , if((table_total - field_cnt)/table_total is null,'100.00%',CONCAT(CAST(round(((table_total - field_cnt)/table_total)*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '" + list.get(i) + "' as field_name,count(*) as table_total, count(case when " + list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp union all ";
                    //pinjie_sql+=" ( select table_name,field_name, "+table_total+" as total_num, field_cnt as empty_num,if(("+table_total+" - field_cnt)/"+table_total+" is null,'100.00%',CONCAT(CAST(round((("+table_total+" - field_cnt)/"+table_total+")*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '"+list.get(i)+"' as field_name,count(case when "+list.get(i)+" is not null then 1 else 0 end) as field_cnt from "+ target_db + "." + target_tbl+" ) as "+target_tbl+String.valueOf(i)+"tmp ) ";
                    continue;
                }

                else {
                    pinjie_sql += " select table_name,field_name,table_total as total_num , field_cnt as empty_num , if((table_total - field_cnt)/table_total is null,'100.00%',CONCAT(CAST(round(((table_total - field_cnt)/table_total)*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '" + list.get(i) + "' as field_name,count(*) as table_total , count(case when " + list.get(i) + " is not null then 1 else 0 end) as field_cnt from " + target_db + "." + target_tbl + " ) as " + target_tbl + String.valueOf(i) + "tmp  union all ";
                }
            }
            spark_session.sql("UNCACHE TABLE "+target_db+"."+target_tbl);
        }
        spark_session.sql("insert into default.result_table_information " +
                " select mysql_data.database_name,mysql_data.table_name,mysql_data.table_comment,mysql_data.column_name,mysql_data.column_comment,mysql_data.integer_idx as field_sort,hive_result_table.total_num,hive_result_table.empty_num,hive_result_table.saturation " +
                " from mysql_data " +
                " inner join  "+pinjie_sql+"  as hive_result_table on mysql_data.table_name=hive_result_table.table_name" +
                " group by mysql_data.database_name,mysql_data.table_name,mysql_data.table_comment,mysql_data.column_name,mysql_data.column_comment,mysql_data.integer_idx,hive_result_table.total_num,hive_result_table.empty_num,hive_result_table.saturation ");
        spark_session.sql("drop table if exists default.result_hive_meta_info");
        spark_session.sql("create table default.result_hive_meta_info(database_name STRING,table_name STRING,table_comment STRING,column_name STRING,column_comment STRING,field_sort BIGINT,total_num BIGINT,empty_num BIGINT,saturation STRING) ");
        spark_session.sql("insert into default.result_hive_meta_info select  database_name, table_name, first(table_comment) as table_comment, column_name column_name, first(column_comment) as column_comment, first(field_sort) as field_sort, first(total_num) as total_num, first(empty_num) as empty_num, first(saturation) as saturation from default.result_table_information group by database_name,table_name,column_name order by table_name,first(field_sort)");
        spark_session.close();
        jsc.close();
    }
}
