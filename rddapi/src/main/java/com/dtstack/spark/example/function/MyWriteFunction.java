package com.dtstack.spark.example.function;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class MyWriteFunction implements Function<String,Row> {

    /**
     * @param line 读取过来的每一行数据 例如:dtsatck 1 1231242342
     * @return
     */
    @Override
    public Row call(String line) throws Exception {
        String[] field = line.split(" ");
        return RowFactory.create(field[0].toString(), Integer.parseInt(field[1]));
    }
}
