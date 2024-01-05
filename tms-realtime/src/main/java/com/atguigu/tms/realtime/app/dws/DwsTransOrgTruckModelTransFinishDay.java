package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransTransFinishBean;
import com.atguigu.tms.realtime.beans.DwsTransOrgTruckModelTransFinishDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

// 物流域机构卡车类别粒度聚合统计
public class DwsTransOrgTruckModelTransFinishDay {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // 从Kafka的运输事实表中读取数据
        String topic = "tms_dwd_trans_trans_finish";
        String groupId = "dws_trans_org_truck_model_group";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaDS = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // 对流中数据进行类型转换 jsonStr->实体类 关联卡车维度
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> mapDS = kafkaDS.map(
                new MapFunction<String, DwsTransOrgTruckModelTransFinishDayBean>() {
                    @Override
                    public DwsTransOrgTruckModelTransFinishDayBean map(String jsonStr) throws Exception {
                        DwdTransTransFinishBean finishBean = JSON.parseObject(jsonStr, DwdTransTransFinishBean.class);
                        DwsTransOrgTruckModelTransFinishDayBean bean = DwsTransOrgTruckModelTransFinishDayBean.builder()
                                .orgId(finishBean.getStartOrgId())
                                .orgName(finishBean.getStartOrgName())
                                .truckId(finishBean.getTruckId())
                                .transFinishCountBase(1L)
                                .transFinishDistanceBase(finishBean.getActualDistance())
                                .transFinishDurTimeBase(finishBean.getTransportTime())
                                .ts(finishBean.getTs() + 8 * 60 * 60 * 1000L)
                                .build();
                        return bean;
                    }
                }
        );


        // 关联卡车维度 获取卡车型号
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withTruckDS = AsyncDataStream.unorderedWait(
                mapDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_truck_info") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setTruckModelId(dimInfoJsonObj.getString("truck_model_id"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id", bean.getTruckId());
                    }
                },
                60, TimeUnit.SECONDS
        );


        // 指定Watermark的生成策略并提起事件时间字段
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withWatermarkDS = withTruckDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransOrgTruckModelTransFinishDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTransOrgTruckModelTransFinishDayBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTransOrgTruckModelTransFinishDayBean element, long l) {
                                        return element.getTs();
                                    }
                                }
                        )
        );


        // 按照机构id + 卡车型号进行分组
        KeyedStream<DwsTransOrgTruckModelTransFinishDayBean, String> keyDS = withWatermarkDS.keyBy(
                new KeySelector<DwsTransOrgTruckModelTransFinishDayBean, String>() {
                    @Override
                    public String getKey(DwsTransOrgTruckModelTransFinishDayBean bean) throws Exception {
                        return bean.getOrgId() + "+" + bean.getTruckModelId();
                    }
                }
        );

        // 开窗
        WindowedStream<DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow> windowDS = keyDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // 指定自定义触发器
        WindowedStream<DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow> triggerDS = windowDS.trigger(new MyTriggerFunction<DwsTransOrgTruckModelTransFinishDayBean>());

        // 聚合
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> aggregateDS = triggerDS.aggregate(
                new MyAggregationFunction<DwsTransOrgTruckModelTransFinishDayBean>() {
                    @Override
                    public DwsTransOrgTruckModelTransFinishDayBean add(DwsTransOrgTruckModelTransFinishDayBean value, DwsTransOrgTruckModelTransFinishDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setTransFinishCountBase(value.getTransFinishCountBase() + accumulator.getTransFinishCountBase());
                        accumulator.setTransFinishDistanceBase(value.getTransFinishDistanceBase().add(accumulator.getTransFinishDistanceBase()));
                        accumulator.setTransFinishDurTimeBase(value.getTransFinishDurTimeBase() + accumulator.getTransFinishDurTimeBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgTruckModelTransFinishDayBean, DwsTransOrgTruckModelTransFinishDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsTransOrgTruckModelTransFinishDayBean> elements, Collector<DwsTransOrgTruckModelTransFinishDayBean> out) throws Exception {
                        Long stt = context.window().getStart() - 8 * 60 * 60 * 1000L;
                        String curDate = DateFormatUtil.toDate(stt);
                        for (DwsTransOrgTruckModelTransFinishDayBean element : elements) {
                            element.setCurDate(curDate);
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );


        // 关联维度信息
        // 获取卡车型号名称
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withTruckModelDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_truck_model") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setTruckModelName(dimInfoJsonObj.getString("model_name"));

                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id", bean.getTruckModelId());
                    }
                },
                60, TimeUnit.SECONDS
        );


        // 获取机构（对应的转运中心）的id
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withJoinOrgIdDS = AsyncDataStream.unorderedWait(
                withTruckModelDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimInfoJsonObj) {
                        String orgParentId = dimInfoJsonObj.getString("org_parent_id");
                        bean.setJoinOrgId(orgParentId != null ? orgParentId : bean.getOrgId());

                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id", bean.getOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        );

//         根据转运中心的id，到机构表中获取城市id
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withCityIdDS = AsyncDataStream.unorderedWait(
                withJoinOrgIdDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setCityId(dimInfoJsonObj.getString("region_id"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id", bean.getJoinOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        );
        ;


//         根据城市id 到区域表中获取城市名称
        SingleOutputStreamOperator<DwsTransOrgTruckModelTransFinishDayBean> withCityNameDS = AsyncDataStream.unorderedWait(
                withCityIdDS,
                new DimAsyncFunction<DwsTransOrgTruckModelTransFinishDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTransOrgTruckModelTransFinishDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setCityName(dimInfoJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgTruckModelTransFinishDayBean bean) {
                        return Tuple2.of("id", bean.getCityId());
                    }
                },
                60, TimeUnit.SECONDS
        );


        // 将结果写入ck
        withCityNameDS.print(">>>");
        withCityNameDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_org_truck_model_trans_finish_day_base values(?,?,?,?,?,?,?,?,?,?,?)")
        );


        env.execute();
    }
}
