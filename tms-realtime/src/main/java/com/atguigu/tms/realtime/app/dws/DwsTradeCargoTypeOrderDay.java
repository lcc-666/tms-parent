package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTradeOrderDetailBean;
import com.atguigu.tms.realtime.beans.DwsTradeCargoTypeOrderDayBean;
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

//交易域:货物类型下单数以及下单金额聚合统计
public class DwsTradeCargoTypeOrderDay {
    public static void main(String[] args) throws Exception {
        // 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // 从Kafka读取数据
        String topic = "tms_dwd_trade_order_detail";
        String groupId = "dws_trade_cargo_type_order_group";

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");


        // 对流中的数据进行类型转换 jsonStr->实体类对象
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> mapDS = kafkaStrDS.map(
                new MapFunction<String, DwsTradeCargoTypeOrderDayBean>() {
                    @Override
                    public DwsTradeCargoTypeOrderDayBean map(String JsonStr) throws Exception {
                        DwdTradeOrderDetailBean dwdTradeOrderDetailBean = JSON.parseObject(JsonStr, DwdTradeOrderDetailBean.class);
                        DwsTradeCargoTypeOrderDayBean bean = DwsTradeCargoTypeOrderDayBean.builder()
                                .cargoType(dwdTradeOrderDetailBean.getCargoType())
                                .orderAmountBase(dwdTradeOrderDetailBean.getAmount())
                                .orderCountBase(1L)
                                .ts(dwdTradeOrderDetailBean.getTs() + 8 * 60 * 60 * 1000)
                                .build();
                        return bean;
                    }
                }
        );

        // 指定Watermark以及提起事件时间字段
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> withWatermarkDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsTradeCargoTypeOrderDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeCargoTypeOrderDayBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeCargoTypeOrderDayBean element, long l) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        // 按照货物类型进行分组
        KeyedStream<DwsTradeCargoTypeOrderDayBean, String> keyedDS = withWatermarkDS.keyBy(DwsTradeCargoTypeOrderDayBean::getCargoType);

        // 开窗
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.days(1)));

        // 指定自定义触发器
        WindowedStream<DwsTradeCargoTypeOrderDayBean, String, TimeWindow> triggerDS = windowDS.trigger(new MyTriggerFunction<DwsTradeCargoTypeOrderDayBean>());

        // 聚合计算
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> aggregateDS = triggerDS.aggregate(
                new MyAggregationFunction<DwsTradeCargoTypeOrderDayBean>() {
                    @Override
                    public DwsTradeCargoTypeOrderDayBean add(DwsTradeCargoTypeOrderDayBean value, DwsTradeCargoTypeOrderDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setOrderAmountBase(value.getOrderAmountBase().add(accumulator.getOrderAmountBase()));
                        accumulator.setOrderCountBase(value.getOrderCountBase() + accumulator.getOrderCountBase());
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTradeCargoTypeOrderDayBean, DwsTradeCargoTypeOrderDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsTradeCargoTypeOrderDayBean, DwsTradeCargoTypeOrderDayBean, String, TimeWindow>.Context context, Iterable<DwsTradeCargoTypeOrderDayBean> elements, Collector<DwsTradeCargoTypeOrderDayBean> out) throws Exception {
                        long sst = context.window().getStart() - 8 * 60 * 60 * 1000L;
                        for (DwsTradeCargoTypeOrderDayBean bean : elements) {
                            String curDate = DateFormatUtil.toDate(sst);
                            bean.setCurDate(curDate);
                            bean.setTs(System.currentTimeMillis());
                            out.collect(bean);
                        }
                    }
                }
        );

        // 关联货物维度
        SingleOutputStreamOperator<DwsTradeCargoTypeOrderDayBean> withCargoTypeDS = AsyncDataStream.unorderedWait(
                aggregateDS,
                new DimAsyncFunction<DwsTradeCargoTypeOrderDayBean>("dim_base_dic") {
                    @Override
                    public void join(DwsTradeCargoTypeOrderDayBean bean, JSONObject dimInfoJsonObj) {
                        bean.setCargoTypeName(dimInfoJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTradeCargoTypeOrderDayBean bean) {
                        return Tuple2.of("id",bean.getCargoType());
                    }
                },
                60,
                TimeUnit.SECONDS
        );


        // 将结果写入ck
        withCargoTypeDS.print(">>>>");
        withCargoTypeDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trade_cargo_type_order_day_base values(?,?,?,?,?,?)")
        );

        env.execute();
    }
}
