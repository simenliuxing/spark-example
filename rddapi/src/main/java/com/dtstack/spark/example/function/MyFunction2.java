package com.dtstack.spark.example.function;

import com.esotericsoftware.minlog.Log;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class MyFunction2 implements Function2<Integer, Integer, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(MyFunction2.class);

    /**
     * @param v1 key对应的上一个value值 例如:1
     * @param v2 key对应的下一个value值 例如:2
     * @return
     */
    @Override
    public Integer call(Integer v1, Integer v2) {
        try {
            return v1 + v2;
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return null;
    }
}
