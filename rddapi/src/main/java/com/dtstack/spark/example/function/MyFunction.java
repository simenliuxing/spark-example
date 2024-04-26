package com.dtstack.spark.example.function;

import com.esotericsoftware.minlog.Log;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class MyFunction implements Function<Row, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MyFunction.class);

    /**
     * @param row 读取过来的每一行数据 例如:dtsatck 1 1231242342
     * @return
     */
    @Override
    public String call(Row row) throws Exception {
        String string="";
        try {
            string = row.getString(0);
            return string;
        }catch (Exception e){
            Log.error(e.toString());
        }
            return null;
    }
}