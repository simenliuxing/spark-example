package com.dtstack.spark.example.function;

import com.esotericsoftware.minlog.Log;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author longxuan
 * @Create 2021/10/10 09:58
 * @Description
 */
public class MysqlData {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlData.class);

    /**
     * @param sparkSession 定义的SparkSession实例
     * @param inpuitSql 需要解析的Sql
     * @return
     */
    public   Dataset<Row> SelecctMysqlData(SparkSession sparkSession,String inpuitSql){
        Dataset<Row> dataset=null;
        try {
            dataset = sparkSession.sql(inpuitSql);
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return dataset;
    }
}
