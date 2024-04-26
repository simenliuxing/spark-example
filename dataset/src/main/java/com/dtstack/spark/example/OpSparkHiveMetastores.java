package com.dtstack.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class OpSparkHiveMetastores {
    public static void main(String[] args) throws AnalysisException {
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
        Dataset<Row> df1 =spark_session.read().jdbc(url, "tbls", prop);
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

        String target_table="select" +
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
                " where t2.NAME='"+target_db+"' and t1.TBL_NAME not like 'select_sql_temp_table%' and t1.TBL_NAME not like 'temp_%'" +
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

        //建立hive表缓存表
        spark_session.sql("drop table if exists default.hive_result_table");
        spark_session.sql("create table default.hive_result_table(table_name STRING,field_name STRING,total_num BIGINT,empty_num BIGINT,saturation STRING)");

        //建立mysql和hive的最终结果表 有数据库名、表名、列名、字段排序、表的总数、空值、饱和度
        spark_session.sql("drop table if exists default.result_table_information");
        spark_session.sql("create table default.result_table_information(database_name STRING,table_name STRING,table_comment STRING,column_name STRING,column_comment STRING,field_sort BIGINT,total_num BIGINT,empty_num BIGINT,saturation STRING)");


        //遍历这个库名的所有表
        for (String target_tbl : table_num) {
            //根据mysql查询的表名去desc hive的列
            Iterator<Row> rowIterator = spark_session.sql("desc "+target_db+"."+target_tbl).toLocalIterator();
            ArrayList<String> list = new ArrayList<String>();
            while (rowIterator.hasNext()){
                String line = (String) rowIterator.next().get(0);
                if(line.startsWith("#")){
                    continue;
                }
                list.add(line);
            }
            //懒缓存
            spark_session.sql("CACHE LAZY TABLE "+target_db+"."+target_tbl);

//            //1.查询出表名+总的count(*)值
            int table_total=0;
            Iterator<Row> total_num = spark_session.sql("SELECT count(*) as total_num " + "from " + target_db + "." + target_tbl).select("total_num").toLocalIterator();
            while (total_num.hasNext()){
                table_total=(Integer)((Long) total_num.next().get(0)).intValue();

            }

            String pinjie_sql="";

            for(int i=0;i<list.size();i++){
                if(i==list.size()-1){//最后一个
                    pinjie_sql+=" select table_name,field_name, "+table_total+" as total_num, field_cnt as empty_num,if(("+table_total+" - field_cnt)/"+table_total+" is null,'100.00%',CONCAT(CAST(round((("+table_total+" - field_cnt)/"+table_total+")*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '"+list.get(i)+"' as field_name,count(case when "+list.get(i)+" is not null then 1 else 0 end) as field_cnt from "+ target_db + "." + target_tbl+" ) as "+target_tbl+String.valueOf(i)+"tmp ";
                    continue;
                }else if (i==0){
                    pinjie_sql+=" INSERT INTO default.hive_result_table select table_name,field_name, "+table_total+" as total_num, field_cnt as empty_num,if(("+table_total+" - field_cnt)/"+table_total+" is null,'100.00%',CONCAT(CAST(round((("+table_total+" - field_cnt)/"+table_total+")*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '"+list.get(i)+"' as field_name,count(case when "+list.get(i)+" is not null then 1 else 0 end) as field_cnt from "+ target_db + "." + target_tbl+" ) as "+target_tbl+String.valueOf(i)+"tmp union all ";
                    continue;
                }
                else{
                    pinjie_sql+=" select table_name,field_name,"+table_total+" as total_num, field_cnt as empty_num,if(("+table_total+" - field_cnt)/"+table_total+" is null,'100.00%',CONCAT(CAST(round((("+table_total+" - field_cnt)/"+table_total+")*100,2) AS STRING),'%')) as saturation from (select '" + target_tbl + "' as table_name, '"+list.get(i)+"' as field_name, count(case when "+list.get(i)+" is not null then 1 else 0 end) as field_cnt from "+ target_db + "." + target_tbl+" ) as "+target_tbl+String.valueOf(i)+"tmp  union all ";
                }
            }

            spark_session.sql(pinjie_sql);
            spark_session.sql("insert into default.result_table_information " +
                    " select mysql_data.database_name,mysql_data.table_name,mysql_data.table_comment,mysql_data.column_name,mysql_data.column_comment,mysql_data.integer_idx as field_sort,hive_result_table.total_num,hive_result_table.empty_num,hive_result_table.saturation " +
                    " from mysql_data " +
                    " inner join hive_result_table on mysql_data.table_name=hive_result_table.table_name" +
                    " group by mysql_data.database_name,mysql_data.table_name,mysql_data.table_comment,mysql_data.column_name,mysql_data.column_comment,mysql_data.integer_idx,hive_result_table.total_num,hive_result_table.empty_num,hive_result_table.saturation ");
            spark_session.sql("select * from default.result_table_information").show();
            spark_session.sql("truncate table default.hive_result_table");
            spark_session.sql("UNCACHE TABLE "+target_db+"."+target_tbl);

        }
        spark_session.close();
        jsc.close();
    }
}
