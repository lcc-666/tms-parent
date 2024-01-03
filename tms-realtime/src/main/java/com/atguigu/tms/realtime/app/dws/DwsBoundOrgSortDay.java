package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdBoundSortBean;
import com.atguigu.tms.realtime.beans.DwsBoundOrgSortDayBean;
import com.atguigu.tms.realtime.utils.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

public class DwsBoundOrgSortDay {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // kafka读取数据
        String topic = "tms_dwd_bound_sort";
        String groupId = "dws_tms_dwd_bound_sort";

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");


        // 对流中的数据进行类型转换 jsonStr-> 实体类
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> dwsBoundOrgSortDayBeanSingleOutputStreamOperator = kafkaStrDS.map(
                new MapFunction<String, DwsBoundOrgSortDayBean>() {
                    @Override
                    public DwsBoundOrgSortDayBean map(String jsonStr) throws Exception {
                        DwdBoundSortBean dwdBoundSortBean = JSON.parseObject(jsonStr, DwdBoundSortBean.class);
                        return DwsBoundOrgSortDayBean.builder()
                                .orgId(dwdBoundSortBean.getOrgId())
                                .sortCountBase(1L)
                                .ts(dwdBoundSortBean.getTs() + 8 * 60 * 60 * 1000L)
                                .build();
                    }
                }
        );


        // 指定Watermark以及提取事件事件字段
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withWatermarkDS = dwsBoundOrgSortDayBeanSingleOutputStreamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsBoundOrgSortDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsBoundOrgSortDayBean>() {
                                    @Override
                                    public long extractTimestamp(DwsBoundOrgSortDayBean boundOrgSortDayBean, long recordTimestamp) {
                                        return boundOrgSortDayBean.getTs();
                                    }
                                }
                        )

        );

//        withWatermarkDS.print("###");

        // 按照机构id进行分组
        KeyedStream<DwsBoundOrgSortDayBean, String> keyedDS = withWatermarkDS.keyBy(DwsBoundOrgSortDayBean::getOrgId);

        // 开窗
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1L)));

        // 指定自定义触发器
        WindowedStream<DwsBoundOrgSortDayBean, String, TimeWindow> triggerDS = windowDS.trigger(new MyTriggerFunction<>());

        // 聚合
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> aggregateDS = triggerDS.aggregate(
                new MyAggregationFunction<DwsBoundOrgSortDayBean>() {
                    @Override
                    public DwsBoundOrgSortDayBean add(DwsBoundOrgSortDayBean value, DwsBoundOrgSortDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setSortCountBase(accumulator.getSortCountBase() + 1);
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsBoundOrgSortDayBean, DwsBoundOrgSortDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<DwsBoundOrgSortDayBean> elements, Collector<DwsBoundOrgSortDayBean> out) throws Exception {
                        for (DwsBoundOrgSortDayBean element : elements) {
                            // 获取窗口起始时间
                            long stt = context.window().getStart();
                            // 将窗口时间左移8小时 并转换格式
                            element.setCurDate(DateFormatUtil.toDate(stt - 8 * 60 * 60 * 1000L));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        );


        // 关联维度（城市、省份）
        // 关联机构维度 获取机构名称
        // 异步I/O
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withOrgNameDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean sortDayBean, JSONObject dimInfoJsonObj) {
                        sortDayBean.setOrgName(dimInfoJsonObj.getString("org_name"));
                        String orgParentId = dimInfoJsonObj.getString("org_parent_id");
                        sortDayBean.setJoinOrgId(orgParentId != null?orgParentId:sortDayBean.getOrgId());
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean sortDayBean) {
                        return Tuple2.of("id", sortDayBean.getOrgId());
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        // 补充城市ID
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityIdDS = AsyncDataStream.unorderedWait(
                withOrgNameDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean sortDayBean, JSONObject dimInfoJsonObj) {
                        sortDayBean.setCityId(dimInfoJsonObj.getString("region_id"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean sortDayBean) {
                        return Tuple2.of("id", sortDayBean.getJoinOrgId());

                    }
                },
                60,
                TimeUnit.SECONDS
        );



        // 关联地区维度表 根据城市的id获取城市名称以及当前城市所属的省份id
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withCityNameAndProvinceIdDS = AsyncDataStream.unorderedWait(
                withCityIdDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean sortDayBean, JSONObject dimInfoJsonObj) {
                        sortDayBean.setCityName(dimInfoJsonObj.getString("name"));
                        sortDayBean.setProvinceId(dimInfoJsonObj.getString("parent_id"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean sortDayBean) {
                        return Tuple2.of("id", sortDayBean.getCityId());

                    }
                },
                60, TimeUnit.SECONDS
        );


        // 关联地区维度表 根据省份的id获取省份的名称
        SingleOutputStreamOperator<DwsBoundOrgSortDayBean> withProvinceDS = AsyncDataStream.unorderedWait(
                withCityNameAndProvinceIdDS,
                new DimAsyncFunction<DwsBoundOrgSortDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsBoundOrgSortDayBean sortDayBean, JSONObject dimInfoJsonObj) {
                        sortDayBean.setProvinceName(dimInfoJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsBoundOrgSortDayBean sortDayBean) {
                        return Tuple2.of("id", sortDayBean.getProvinceId());
                    }
                },
                60, TimeUnit.SECONDS

        );

        withProvinceDS.print(">>>>");


        // 将关联的结果写入ck中
        withProvinceDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_bound_org_sort_day_base values(?,?,?,?,?,?,?,?,?)")
        );


        env.execute();
    }
}
