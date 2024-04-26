package com.dtstack.spark.example.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class MyFlatMapFunction implements FlatMapFunction<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MyFlatMapFunction.class);

    /**
     * @param line 输入的每一行的数据 例如:hello hi
     * @return
     */
    @Override
    public Iterator<String> call(String line) {
        try {
            String[] splited = line.split(",");
            return Arrays.asList(splited).iterator();
        } catch (Exception e) {
            LOG.error(e.toString());
        }
        return null;
    }
}
