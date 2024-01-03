package com.atguigu.tms.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.commom.TmsConfig;
import com.atguigu.tms.realtime.utils.DimUtil;
import com.atguigu.tms.realtime.utils.HbaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Put;

import java.util.Map;
import java.util.Set;

public class DimSinkFunction implements SinkFunction<JSONObject> {
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取输出目的地表名和rowKey
        String sinkTable = jsonObj.getString("sink_table");
        String sinkPk = jsonObj.getString("sink_pk");
        jsonObj.remove("sink_table");
        jsonObj.remove("sink_pk");

        String op = jsonObj.getString("op");
        jsonObj.remove("op");

        JSONObject foreignKeyJsonObj = jsonObj.getJSONObject("foreign_key");
        jsonObj.remove("foreign_key");

        // 获取json中的每一个键值对
        Set<Map.Entry<String, Object>> entrySet = jsonObj.entrySet();
        Put put = new Put(jsonObj.getString(sinkPk).getBytes());
        for (Map.Entry<String, Object> entry : entrySet) {
            if (!sinkPk.equals(entry.getKey())) {
                put.addColumn("info".getBytes(), entry.getKey().getBytes(), entry.getValue().toString().getBytes());
            }
        }
        System.out.println("向hbase表中插入数据");
        HbaseUtil.putPow(TmsConfig.HBASE_NAMESPACE, sinkTable, put);

        // 如果维度数据发生了变化，将Redis中缓存的维度数据清空掉
        if ("u".equals(op)) {
            // 删除当前维度数据在Redis中对应主键的缓存
            DimUtil.delCached(sinkTable, Tuple2.of("id", jsonObj.getString("id")));
            // 删除当前维度数据在Redis中对应外键的缓存
            Set<Map.Entry<String, Object>> set = foreignKeyJsonObj.entrySet();

            for (Map.Entry<String, Object> entry : set) {
                DimUtil.delCached(sinkTable, Tuple2.of(entry.getKey(), entry.getValue().toString()));
            }
        }
    }
}
