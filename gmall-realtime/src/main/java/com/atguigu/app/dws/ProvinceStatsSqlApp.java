package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class ProvinceStatsSqlApp {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";
        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `order_id` BIGINT, " +
                "  `split_total_amount` DECIMAL, " +
                "  `create_time` STRING, " +
                "  `rt` as TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND ) with(" +
                MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

        Table table = tableEnv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    count(distinct order_id) order_count, " +
                "    sum(split_total_amount) order_amount, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from " +
                "    order_wide " +
                "group by " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(table, ProvinceStats.class);
        provinceStatsDataStream.print();
        provinceStatsDataStream.addSink(ClickHouseUtil.getSink("insert into province_stats_210325 values(?,?,?,?,?,?,?,?,?,?)"));


        env.execute("ProvinceStatsSqlApp");
    }
}
