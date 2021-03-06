package com.atguigu.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String id = getKey(t);

                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    if (dimInfo != null) {
                        join(t, dimInfo);
                    }
                    resultFuture.complete(Collections.singleton(t));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("time out " + input);

    }
}
