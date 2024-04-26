package com.dtstack.spark.example;
import com.dtstack.spark.example.function.MysqlData;
import com.esotericsoftware.minlog.Log;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Catalog;
import org.apache.spark.sql.catalog.Column;
import java.util.*;
/**
 * @Author longxuan
 * @Create 2021/10/10 09:58
 * @Description
 */
public class SparkHiveMetastores {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkHiveMetastores").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).appName("SparkHive_Metastores").config("spark.some.config.option", "some-value").enableHiveSupport().getOrCreate();
        //开启spark支持笛卡尔积
        sparkSession.conf().set("spark.sql.crossJoin.enabled", true);
        Catalog catalog1 = sparkSession.catalog();
        //mysql的jdbc
        String url = "jdbc:mysql://ip:port/database?characterEncoding=utf-8&useSSL=false";
        //库名通过args参数传播
        String target_db = String.valueOf(args[0]);
        Properties prop = new Properties();
        prop.setProperty("user", "*********");
        prop.setProperty("password", "***********");
        sparkSession.read().jdbc(url, "tbls", prop).createOrReplaceTempView("tbls");
        sparkSession.read().jdbc(url, "dbs", prop).createOrReplaceTempView("dbs");
        sparkSession.read().jdbc(url, "table_params", prop).createOrReplaceTempView("table_params");
        sparkSession.read().jdbc(url, "sds", prop).createOrReplaceTempView("sds");
        sparkSession.read().jdbc(url, "columns_v2", prop).createOrReplaceTempView("columns_v2");
        //1.根据库名从mysql查询表，得到表的拥有者、数据库名、表名、列名、字段类型
        String sql_q="select database_name,table_name,table_comment,column_name,column_comment from (select database_name, table_name,table_comment,column_name,column_comment, row_number() over(partition by database_name,table_name,column_name order by table_comment,column_comment desc) as row_number from (SELECT" +
                "  t2.NAME database_name," +
                "  t1.TBL_NAME table_name, " +
                "  t3.PARAM_VALUE table_comment, " +
                "  t5.COLUMN_NAME column_name, " +
                "  t5.COMMENT column_comment " +
                " FROM " +
                "  tbls t1 " +
                " JOIN " +
                "  dbs t2 " +
                " ON " +
                "  t1.DB_ID = t2.DB_ID " +
                " JOIN " +
                " table_params t3 " +
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
                " group by t2.NAME,t1.TBL_NAME,t3.PARAM_VALUE,t5.COLUMN_NAME,t5.COMMENT) temp)temp2 where row_number=1";
        Dataset<Row> dataset = new MysqlData().SelecctMysqlData(sparkSession, sql_q);
        dataset.createOrReplaceTempView("mysql_data");
        //根据传递进来的数据库名查到这个数据库下对应的所有表,会有重复
        Set<String> staffsSet = new HashSet<String>();
        //去mysql里查询表
        java.util.Iterator<Row> target_table_name = new MysqlData().SelecctMysqlData(sparkSession, sql_q).select("table_name").toLocalIterator();
        while (target_table_name.hasNext()) {
            //表有重复的，所以用set过滤
            Row next = target_table_name.next();
            String name = (String) next.get(0);
            staffsSet.add(name.toLowerCase());
        }
        //要查询数据库的目标表
        ArrayList<String> table_num = new ArrayList<String>(staffsSet);
        //1.建立总表缓存
        sparkSession.sql("drop table if exists default.total_data");
        sparkSession.sql("create table default.total_data(table_name STRING,total_num BIGINT)");
        //1.建立字段缓存
        sparkSession.sql("drop table if exists default.num_temp_result");
        sparkSession.sql("create table default.num_temp_result(table_name STRING,field_name STRING,empty_num BIGINT)");
        //建立hive表缓存表
        sparkSession.sql("drop table if exists default.hive_result_table");
        sparkSession.sql("create table default.hive_result_table(table_name STRING,total_num BIGINT,field_name STRING,empty_num BIGINT,saturation STRING)");
        //建立mysql和hive的最终结果表 有数据库名、表名、列名、字段排序、表的总数、空值、饱和度
        sparkSession.sql("drop table if exists default.result_table_information");
        sparkSession.sql("create table default.result_table_information(database_name STRING,table_name STRING,table_comment STRING,column_name STRING,column_comment STRING,field_sort BIGINT,total_num BIGINT,empty_num BIGINT,saturation STRING)");
        //遍历这个库名的所有表
        for (String target_tbl : table_num) {
            ArrayList<String> list = new ArrayList<String>();
            Iterator<Column> columnIterator1 = null;
            //字段集合  根据mysql查询的表去hive里查询,过滤分区字段
            try {
                columnIterator1 = catalog1.listColumns(target_db, target_tbl).toLocalIterator();
            }catch (Exception e){
                Log.error(e.toString());
            }
            while (columnIterator1.hasNext()) {
                String name = columnIterator1.next().name();
                if ("pt".equals(name.toLowerCase())) {
                    continue;
                }
                list.add(name);
            }
            //1.查询出表名+总的count(*)值
            sparkSession.sql("INSERT INTO default.total_data SELECT '" + target_tbl + "' as table_name," + "count(*) as total_num " + "from " + target_db + "." + target_tbl);
            for(int i=0;i<list.size();i++){
                //查询出1条就插入一条数据 最终这个是遍历完字段就把这个default.num_temp_result存满
                sparkSession.sql("INSERT INTO default.num_temp_result SELECT '" + target_tbl + "' as table_name, '" +list.get(i)+ "' as field_name,count(*) as empty_num " + "from " + target_db + "." + target_tbl + " where " + list.get(i) + " is null");
            }
            sparkSession.sql(
                    "INSERT INTO default.hive_result_table select " +
                            "t1.table_name,t1.total_num," +
                            "t2.field_name,t2.empty_num," +
                            "if((t1.total_num-t2.empty_num)/t1.total_num is null,'100.00%',CONCAT(CAST(round(((t1.total_num-t2.empty_num)/t1.total_num)*100,2) AS VARCHAR),'%')) as saturation " +
                            "from default.total_data t1 inner join default.num_temp_result t2 on t1.table_name=t2.table_name"
            );
            sparkSession.sql("INSERT INTO default.result_table_information " +
                    " select ta2.database_name,ta2.table_name,ta2.table_comment,ta2.column_name,column_comment,ta2.field_sort,ta2.total_num,ta2.empty_num,ta2.saturation from (select ta1.database_name,ta1.table_name,ta1.table_comment,ta1.column_name,ta1.column_comment,ta1.total_num,ta1.empty_num,ta1.saturation,row_number() over(partition by ta1.table_name order by ta1.COLUMN_NAME) as field_sort from (select mysql_data.database_name,mysql_data.table_name,mysql_data.table_comment,mysql_data.column_name,mysql_data.column_comment,hive_result_table.total_num,hive_result_table.empty_num,hive_result_table.saturation from mysql_data  inner join hive_result_table on mysql_data.table_name=hive_result_table.table_name  group by mysql_data.database_name,mysql_data.table_name,mysql_data.table_comment,mysql_data.column_name,mysql_data.column_comment,hive_result_table.total_num,hive_result_table.empty_num,hive_result_table.saturation) as ta1) as ta2");
            sparkSession.sql("select * from default.result_table_information").show();
            sparkSession.sql("truncate table default.total_data");
            sparkSession.sql("truncate table default.num_temp_result");
            sparkSession.sql("truncate table default.hive_result_table");
        }
        sparkSession.close();
        jsc.close();
    }
}
