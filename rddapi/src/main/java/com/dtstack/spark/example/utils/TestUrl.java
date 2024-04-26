package com.dtstack.spark.example.utils;
import java.net.URI;
import java.net.URISyntaxException;

import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @Author longxuan
 * @Create 2021/11/11 09:58
 * @Description
 */

public class TestUrl {
    private static final Logger LOG = LoggerFactory.getLogger(TestUrl.class);
    private final static String resultHdfsRoot = "hdfs://172.16.20.10:9000";

    /**
     * @param hdfsRoot 传入的hdfs地址
     * @return
     */
    public URI getUrl(String hdfsRoot) {
        URI fsUri = null;
        if ("".equals(hdfsRoot)) {
            try {
                return new URI(resultHdfsRoot);
            } catch (URISyntaxException e) {
                Log.error(e.toString());
            }
        } else {
            try {
                return new URI(hdfsRoot);
            } catch (URISyntaxException e) {
                Log.error(e.toString());
            }
        }
        return null;
    }

}
