package com.dtstack.spark.udf.function;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */

public class MyFunction implements UDF1<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MyFunction.class);

    /**
     * @param line 输入的String类型的字段名
     * @return
     */

    @Override
    public String call(String line) {
        String s = "";
        if ("".equals(line) || line.equals(null)) {
            s = "";
        } else {
            try {
                s = line.toUpperCase();
            } catch (Exception e) {
                LOG.error(e.toString());
            }
        }
        return s;
    }
}