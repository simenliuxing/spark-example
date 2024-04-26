package com.dtstack.spark.example.utils;
import com.dtstack.spark.example.function.MyVoidFunction;
import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @Author longxuan
 * @Create 2021/9/27 09:58
 * @Description
 */
public class TestPath {
    private static final Logger LOG = LoggerFactory.getLogger(MyVoidFunction.class);

    private final static String hdfsRoot="hdfs://172.16.20.10:9000";
    String start="hdfs://";

    /**
     * @param path 传入的文件地址
     * @return
     */
    public Path getPath(String path){
        if(path.toLowerCase().startsWith(start)){
            try {
                return  new Path(path);
            }catch (Exception e){
            }
        }else {
            try {
                return new Path(hdfsRoot + path);
            }catch (Exception e){
                Log.error(e.toString());
            }
        }
        return null;
    }
}
