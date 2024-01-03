package com.atguigu.tms.realtime.beans;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

public interface DimJoinFunction<T> {

    void join(T obj, JSONObject dimInfoJsonObj);

    Tuple2<String, String> getCondition(T obj);
}
