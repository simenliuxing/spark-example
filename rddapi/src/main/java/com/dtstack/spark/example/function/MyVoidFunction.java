package com.dtstack.spark.example.function;

import com.esotericsoftware.minlog.Log;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class MyVoidFunction implements VoidFunction<Tuple2<String, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(MyVoidFunction.class);

    /**
     * @param tuple2 最终输出的每个元祖，例如:hello 2
     * @return
     */
    @Override
    public void call(Tuple2<String, Integer> tuple2) {
        try {
            System.out.println(tuple2._1 + " " + tuple2._2);
        } catch (Exception e) {
            Log.error(e.toString());
        }
    }
}
