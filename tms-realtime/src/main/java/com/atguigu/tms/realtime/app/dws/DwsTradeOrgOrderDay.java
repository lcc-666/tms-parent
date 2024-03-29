package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.beans.DwsTradeOrgOrderDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
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

// 交易域：机构粒度下单聚合统计
public class DwsTradeOrgOrderDay {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // 从kafka的下单实时表中读取数据
        String topic = "tms_dwd_trade_order_detail";
        String groupId = "dws_trade_org_order_group";

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // 对读取的数据进行类型转换
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> mapDS = kafkaStrDS.map(
                new MapFunction<String, DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean map(String jsonStr) throws Exception {
                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = JSON.parseObject(jsonStr, DwdTradeOrderDetailBean.class);
                        DwsTradeOrgOrderDayBean bean = DwsTradeOrgOrderDayBean.builder()
                                .senderDistrictId(dwdTradeOrderDetailBean.getSenderDistrictId())
                                .cityId(dwdTradeOrderDetailBean.getSenderCityId())
                                .orderAmountBase(dwdTradeOrderDetailBean.getAmount())
                                .orderCountBase(1L)
                                .ts(dwdTradeOrderDetailBean.getTs())
                                .build();

                        return bean;
                    }
                }
        );

        // 关联机构维度
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withOrgDS = AsyncDataStream.unorderedWait(
                mapDS,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setOrgId(dimInfoJsonObj.getString("id"));
                        bean.setOrgName(dimInfoJsonObj.getString("org_name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeOrgOrderDayBean bean) {
                        return Tuple2.of("region_id", bean.getSenderDistrictId());
                    }
                },
                60, TimeUnit.SECONDS
        );


        // 指定Watermark
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withWatermarkDS = withOrgDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsTradeOrgOrderDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeOrgOrderDayBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeOrgOrderDayBean element, long l) {
                                        return element.getTs();
                                    }
                                }
                        )

        );

        // 按照机构id进行分组
        KeyedStream<DwsTradeOrgOrderDayBean, String> keyedDS = withWatermarkDS.keyBy(DwsTradeOrgOrderDayBean::getOrgId);

        // 开窗
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // 指定自定义触发器
        WindowedStream<DwsTradeOrgOrderDayBean, String, TimeWindow> triggerDS = windowDS.trigger(new MyTriggerFunction<DwsTradeOrgOrderDayBean>());

        // 聚合
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> aggregateDS = triggerDS.aggregate(
                new MyAggregationFunction<DwsTradeOrgOrderDayBean>() {
                    @Override
                    public DwsTradeOrgOrderDayBean add(DwsTradeOrgOrderDayBean value, DwsTradeOrgOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setOrderAmountBase(value.getOrderAmountBase().add(accumulator.getOrderAmountBase()));
                        accumulator.setOrderCountBase(value.getOrderCountBase() + accumulator.getOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsTradeOrgOrderDayBean, DwsTradeOrgOrderDayBean, String, TimeWindow>.Context context, Iterable<DwsTradeOrgOrderDayBean> elements, Collector<DwsTradeOrgOrderDayBean> out) throws Exception {
                        long stt = context.window().getStart() - 8 * 60 * 60 * 1000;
                        String curDare = DateFormatUtil.toDate(stt);
                        for (DwsTradeOrgOrderDayBean bean : elements) {
                            bean.setCurDate(curDare);
                            bean.setTs(System.currentTimeMillis());
                            out.collect(bean);
                        }
                    }
                }
        );

        // 补充城市维度信息
        SingleOutputStreamOperator<DwsTradeOrgOrderDayBean> withCityDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTradeOrgOrderDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTradeOrgOrderDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setCityName(dimInfoJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeOrgOrderDayBean bean) {
                        return Tuple2.of("id", bean.getCityId());
                    }
                },
                60, TimeUnit.SECONDS
        );

        // 将结果写入ck中

        withCityDS.print(">>>");
        withCityDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_org_order_day_base values(?,?,?,?,?,?,?,?)")
        );


        env.execute();

    }
}
