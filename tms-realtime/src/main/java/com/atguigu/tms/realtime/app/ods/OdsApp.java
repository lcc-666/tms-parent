package com.atguigu.tms.realtime.app.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OdsApp {
    public static void main(String[] args) throws Exception {
        // 1.获取流处理环境并指定检查点
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);


        // 2 使用FlinkCDC从MySQL中读取数据-事实数据
        String dwdOption = "dwd";
        String dwdServerId = "6030";
        String dwdsourceName = "ods_app_dwd_source";

        mysqlToKafka(dwdOption, dwdServerId, dwdsourceName, env, args);

        // 3 使用FlinkCDC从MySQL中读取数据-维度数据
        String realtimeDimOption = "realtime_dim";
        String realtimeDimServerId = "6040";
        String realtimeDimsourceName = "ods_app_realtimeDim_source";

        mysqlToKafka(realtimeDimOption, realtimeDimServerId, realtimeDimsourceName, env, args);

        env.execute();


    }

    public static void mysqlToKafka(String option, String serverId, String sourceName, StreamExecutionEnvironment env, String[] args) {

        MySqlSource<String> MySqlSource = CreateEnvUtil.getMysqlSource(option, serverId, args);

        SingleOutputStreamOperator<String> dwdStrDS = env.fromSource(MySqlSource, WatermarkStrategy.noWatermarks(), sourceName)
                .setParallelism(1)
                .uid(option + sourceName);


        // 3 简单ETL
        SingleOutputStreamOperator<String> processDS = dwdStrDS.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, String>.Context ctx, Collector<String> out) {
                        try {
                            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                            if (jsonObj.getJSONObject("after") != null && !"d".equals(jsonObj.getString("op"))) {
//                                System.out.println(jsonObj);
                                Long tsMs = jsonObj.getLong("ts_ms");
                                jsonObj.put("ts", tsMs);
                                jsonObj.remove("ts_ms");
                                String jsonString = jsonObj.toJSONString();
                                out.collect(jsonString);
                            }

                        } catch (Exception e) {
                            Log.error("从Flink-CDC得到的数据不是一个标准的json格式",e);
                        }
                    }
                }
        ).setParallelism(1);
        // 4 按照主键进行分组，避免出现乱序
        KeyedStream<String, String> keyedDS = processDS.keyBy((KeySelector<String, String>) jsonStr -> {
            JSONObject jsonObj = JSON.parseObject(jsonStr);
            return jsonObj.getJSONObject("after").getString("id");
        });

        //将数据写入Kafka

        keyedDS.sinkTo(KafkaUtil.getKafkaSink("tms_ods", sourceName + "_transPre", args))
                .uid(option + "_ods_app_sink");
    }
}
