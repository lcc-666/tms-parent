package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.DwdTransTransFinishBean;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

// 物流域：运输完成事实表
public class DwdTransTransFinish {
    public static void main(String[] args) throws Exception {
        // 准备环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // 从kafka读取数据
        String topic = "tms_ods";
        String groupId = "dwd_trans_tran_finish_group";
        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_source")
                .uid("Kafka_source");

        // 筛选出运输完成的数据
        SingleOutputStreamOperator<String> filterDS = kafkaStrDS.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 将transport_task 表的操作数据过滤处理
                        String table = jsonObj.getJSONObject("source").getString("table");
                        if (!"transport_task".equals(table)) {
                            return false;
                        }
                        String op = jsonObj.getString("op");
                        JSONObject beforeJsonObj = jsonObj.getJSONObject("before");
                        if (beforeJsonObj == null) {
                            return false;
                        }
                        JSONObject afterJsonObj = jsonObj.getJSONObject("after");
                        String oldActualEndTime = beforeJsonObj.getString("actual_end_time");
                        String newActualEndTime = afterJsonObj.getString("actual_end_time");

                        return "u".equals(op) && oldActualEndTime == null && newActualEndTime != null;
                    }
                }
        );


        // 筛选出的数据进行处理
        SingleOutputStreamOperator<String> processDS = filterDS.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        DwdTransTransFinishBean finishBean = jsonObj.getObject("after", DwdTransTransFinishBean.class);

                        // 补充运输时常字段
                        finishBean.setTransportTime(Long.parseLong(finishBean.getActualStartTime()) - Long.parseLong(finishBean.getActualEndTime()));

                        // 将运输结束时间转换为毫秒-8小时 赋值给事件时间字段ts
                        finishBean.setTs(Long.parseLong(finishBean.getActualEndTime()) - 8 * 60 * 60 * 1000L);

                        // 处理时间问题
                        finishBean.setActualStartTime(DateFormatUtil.toYmdHms(Long.parseLong(finishBean.getActualStartTime()) - 8 * 60 * 60 * 1000L));
                        finishBean.setActualEndTime(DateFormatUtil.toYmdHms(Long.parseLong(finishBean.getActualEndTime()) - 8 * 60 * 60 * 1000L));

                        // 脱敏
                        String driver1Name = finishBean.getDriver1Name();
                        String driver2Name = finishBean.getDriver2Name();
                        String truckNo = finishBean.getTruckNo();

                        driver1Name = driver1Name.charAt(0) +
                                driver1Name.substring(1).replaceAll(".", "\\*");
                        driver2Name = driver2Name == null ? driver2Name : driver2Name.charAt(0) +
                                driver2Name.substring(1).replaceAll(".", "\\*");
                        truckNo = DigestUtils.md5Hex(truckNo);

                        finishBean.setDriver1Name(driver1Name);
                        finishBean.setDriver2Name(driver2Name);
                        finishBean.setTruckNo(truckNo);

                        out.collect(JSON.toJSONString(finishBean));
                    }
                }
        );
        // 处理后的数据写入kafka
        String sinkTopic = "tms_dwd_trans_trans_finish";
        processDS.sinkTo(KafkaUtil.getKafkaSink(sinkTopic,args)).uid("kafka_sink");

        env.execute();
    }
}
