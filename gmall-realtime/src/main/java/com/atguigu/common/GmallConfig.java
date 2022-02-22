package com.atguigu.common;

public class GmallConfig {
    public static final String HBASE_SCHEMA = "GMALL210325_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER =
            "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

}
