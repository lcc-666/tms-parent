package com.atguigu.tms.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.beans.DwdBoundInboundBean;
import com.atguigu.tms.realtime.beans.DwdBoundOutboundBean;
import com.atguigu.tms.realtime.beans.DwdBoundSortBean;
import com.atguigu.tms.realtime.beans.DwdOrderOrgBoundOriginBean;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdBoundRelevantApp {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // 从kafka读取数据
        String topic = "tms_ods";
        String groupId = "dwd_bound_group";

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // 筛选出订单机构中转表
        SingleOutputStreamOperator<String> filterDS = kafkaStrDS.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String table = jsonObj.getJSONObject("source").getString("table");
                        return "order_org_bound".equals(table);
                    }
                }
        );


        // 定义侧输出流标签
        OutputTag<String> sortTag = new OutputTag<String>("sortTag") {
        };
        OutputTag<String> outboundTag = new OutputTag<String>("outboundTag") {
        };



        // 分流 入库->主流  分流->分拣侧输出流 出库->出库侧输出流
        SingleOutputStreamOperator<String> inboundDS = filterDS.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String jsonStr, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                String op = jsonObj.getString("op");

                DwdOrderOrgBoundOriginBean after = jsonObj.getObject("after", DwdOrderOrgBoundOriginBean.class);
                DwdOrderOrgBoundOriginBean before = jsonObj.getObject("before", DwdOrderOrgBoundOriginBean.class);

                // 获取中转数据id
                String id = after.getId();
                // 获取运单id
                String orderId = after.getOrderId();
                // 获取机构id
                String orgId = after.getOrgId();

                if ("c".equals(op)) {
                    long ts = Long.parseLong(after.getInboundEmpId()) - 8 * 60 * 60 * 1000L;
                    String inboundTime = DateFormatUtil.toYmdHms(ts);
                    String inboundEmpId = after.getInboundEmpId();

                    // 入库
                    DwdBoundInboundBean inboundBean = DwdBoundInboundBean.builder()
                            .id(id)
                            .orderId(orderId)
                            .orgId(orgId)
                            .inboundTime(inboundTime)
                            .inboundEmpId(inboundEmpId)
                            .ts(ts)
                            .build();
                    out.collect(JSON.toJSONString(inboundBean));
                } else {
                    // 将分拣数据放到侧输出流
                    String beforeSortTime = before.getSortTime();
                    String afterSortTime = after.getSortTime();
                    if (beforeSortTime == null && afterSortTime != null) {
                        long ts = Long.parseLong(after.getSortTime()) - 8 * 60 * 60 * 1000L;
                        String sortTime = DateFormatUtil.toYmdHms(ts);
                        String sorterEmpId = after.getSorterEmpId();
                        DwdBoundSortBean sortBean = DwdBoundSortBean.builder()
                                .id(id)
                                .orderId(orderId)
                                .orgId(orgId)
                                .sortTime(sortTime)
                                .sorterEmpId(sorterEmpId)
                                .ts(ts)
                                .build();
                        ctx.output(sortTag, JSON.toJSONString(sortBean));
                    }

                    // 筛选储库操作 将数据库放到出库侧输出流
                    String beforeOutboundTime = before.getOutboundTime();
                    String afterOutboundTime = after.getOutboundTime();

                    if (beforeOutboundTime == null && afterOutboundTime != null) {
                        long ts = Long.parseLong(after.getOutboundTime()) - 8 * 60 * 60 * 1000L;
                        String outboundTime = DateFormatUtil.toYmdHms(ts);
                        String outboundEmpId = after.getOutboundEmpId();

                        DwdBoundOutboundBean outboundBean = DwdBoundOutboundBean.builder()
                                .id(id)
                                .orderId(orderId)
                                .orgId(orgId)
                                .outboundTime(outboundTime)
                                .outboundEmpId(outboundEmpId)
                                .ts(ts)
                                .build();
                        ctx.output(outboundTag, JSON.toJSONString(outboundBean));
                    }

                }

            }
        });

        // 从主流中提取侧输出流
        // 分拣流
        SideOutputDataStream<String> sortDS = inboundDS.getSideOutput(sortTag);
        // 出库流
        SideOutputDataStream<String> outboundDS = inboundDS.getSideOutput(outboundTag);

        // 将不同流数据写到kafka主题
        //中转域入库事实主题
        String inboundTopic = "tms_dwd_bound_inbound";
        //中转域分拣事实主题
        String sortTopic = "tms_dwd_bound_sort";
        //中转域出库事实主题
        String outboundTopic = "tms_dwd_bound_outbound";

        inboundDS.sinkTo(KafkaUtil.getKafkaSink(inboundTopic, args)).uid("inbound_sink");
        sortDS.print(">>>");
        sortDS.sinkTo(KafkaUtil.getKafkaSink(sortTopic, args)).uid("sort_sink");
        outboundDS.sinkTo(KafkaUtil.getKafkaSink(outboundTopic, args)).uid("outbound_sink");


        env.execute();
    }
}
