package com.atguigu.tms.realtime.app.dws;


import com.alibaba.fastjson.JSON;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransDispatchDetailBean;
import com.atguigu.tms.realtime.beans.DwsTransDispatchDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * 物流域发单聚合统计
 */
public class DwsTransDispatchDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trans_dispatch_detail 主题读取数据
        String topic = "tms_dwd_trans_dispatch_detail";
        String groupId = "dws_trans_dispatch_day";

        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransDispatchDayBean> mappedStream = source.map(jsonStr -> {
            DwdTransDispatchDetailBean dispatchDetailBean = JSON.parseObject(jsonStr, DwdTransDispatchDetailBean.class);
            return DwsTransDispatchDayBean.builder()
                    .dispatchOrderCountBase(1L)
                    .ts(dispatchDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<DwsTransDispatchDayBean> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<DwsTransDispatchDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                WatermarkStrategy.<DwsTransDispatchDayBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransDispatchDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTransDispatchDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        ).uid("watermark_stream");

        // TODO 5. 开窗
        AllWindowedStream<DwsTransDispatchDayBean, TimeWindow> windowedStream =
                withWatermarkStream.windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 6. 引入触发器
        AllWindowedStream<DwsTransDispatchDayBean, TimeWindow> triggerStream = windowedStream.trigger(
                new MyTriggerFunction<DwsTransDispatchDayBean>()
        );

        // TODO 7. 聚合
        SingleOutputStreamOperator<DwsTransDispatchDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransDispatchDayBean>() {
                    @Override
                    public DwsTransDispatchDayBean add(DwsTransDispatchDayBean value, DwsTransDispatchDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setDispatchOrderCountBase(
                                accumulator.getDispatchOrderCountBase() + value.getDispatchOrderCountBase()
                        );
                        return accumulator;
                    }
                },
                new ProcessAllWindowFunction<DwsTransDispatchDayBean, DwsTransDispatchDayBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<DwsTransDispatchDayBean> elements, Collector<DwsTransDispatchDayBean> out) throws Exception {
                        for (DwsTransDispatchDayBean element : elements) {
                            String curDate = DateFormatUtil.toDate(context.window().getStart() - 8 * 60 * 60 * 1000L);
                            // 补充统计日期字段
                            element.setCurDate(curDate);
                            // 补充时间戳字段
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 8. 写出到 ClickHouse
        aggregatedStream.print(">>>>");
        aggregatedStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_dispatch_day_base values(?,?,?)")
        ).uid("clickhouse_stream");

        env.execute();
    }
}