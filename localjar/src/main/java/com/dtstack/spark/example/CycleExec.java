/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.spark.example;

import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author chuixue
 * @create 2023-01-31 13:29
 * @description 轮训判断某个条件是否符合要求，符合流程往下走，不符合一直轮训
 * todo 因为数栈不支持直接运行java，此处使用spark包装，在spark的driver上，运行成功后执行后续依赖任务
 **/
public class CycleExec {
    public static void main(String[] args) throws Exception {
        System.out.println("======= start =======");

        SparkSession sparkSession = SparkSession.builder()
                .appName("轮训数据是否准备好")
                // .master("local[1]") // 本地调试用，提交到yarn需要注释掉
                .getOrCreate();

        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            String url = "jdbc:oracle:thin:@172.16.100.243:1521:orcl";
            conn = DriverManager.getConnection(url, "oracle", "oracle");
            st = conn.createStatement();

            while (true) {
                // 1.sql取数条件
                rs = st.executeQuery("select count(*) from policy_type");
                int conditions = 0;
                while (rs.next()) {
                    conditions = rs.getInt(1);
                }

                // 2.退出轮训条件
                if (conditions >= 3) {
                    System.out.println("===== " + conditions + " 符合条件，退出循环");
                    break;
                } else {
                    System.out.println("===== " + conditions + " 不符合条件，继续循环");
                }
                rs.close();

                // 3.轮训频率
                Thread.sleep(3000);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

        System.out.println("======= end =======");
        sparkSession.close();
    }
}
