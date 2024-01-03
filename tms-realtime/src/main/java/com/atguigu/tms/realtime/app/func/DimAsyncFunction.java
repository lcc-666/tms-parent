package com.atguigu.tms.realtime.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.DimJoinFunction;
import com.atguigu.tms.realtime.commom.TmsConfig;
import com.atguigu.tms.realtime.utils.DimUtil;
import com.atguigu.tms.realtime.utils.ThreadPoolUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }


    private ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        // 从线程池中获取线程，发送异步请求
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        // 根据流中的对象获取要作为查询条件的主键或者外键
                        Tuple2<String, String> keyNameAndValue = getCondition(obj);
                        // 根据查询条件获取维度对象
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(TmsConfig.HBASE_NAMESPACE, tableName, keyNameAndValue);
                        // 将维度对象的属性补充到流中的对象上
                        if (dimInfoJsonObj != null) {
                            join(obj, dimInfoJsonObj);
                        }
                        // 向下游传递数据
                        resultFuture.complete(Collections.singleton(obj));

                    }
                }
        );
    }
}
