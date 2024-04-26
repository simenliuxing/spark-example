package com.dtstack.spark.example.function;

import com.esotericsoftware.minlog.Log;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class MyPairFunction implements PairFunction<String, String, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(MyPairFunction.class);

    /**
     * @param word 每行数据压平分割后的每个小字符串 例如:hello
     * @return
     */
    @Override
    public Tuple2<String, Integer> call(String word) {
        try {
            return new Tuple2<>(word, 1);
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return null;
    }
}
