package com.atguigu.tms.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimAsyncFunction;
import com.atguigu.tms.realtime.app.func.MyAggregationFunction;
import com.atguigu.tms.realtime.app.func.MyTriggerFunction;
import com.atguigu.tms.realtime.beans.DwdTransDeliverSucDetailBean;
import com.atguigu.tms.realtime.beans.DwsTransOrgDeliverSucDayBean;
import com.atguigu.tms.realtime.utils.ClickHouseUtil;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.DateFormatUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 物流域机构派送成功统计
 */
public class DwsTransOrgDeliverSucDay {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);

        // 并行度设置，部署时应注释，通过 args 指定全局并行度
        env.setParallelism(4);

        // TODO 2. 从 Kafka tms_dwd_trans_deliver_detail 主题读取数据
        String topic = "tms_dwd_trans_deliver_detail";
        String groupId = "dws_trans_org_deliver_suc_day";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaSource(topic, groupId, args);
        SingleOutputStreamOperator<String> source = env
                .fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "kafka_source")
                .uid("kafka_source");

        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> mappedStream = source.map(jsonStr -> {
            DwdTransDeliverSucDetailBean dwdTransDeliverSucDetailBean = JSON.parseObject(jsonStr, DwdTransDeliverSucDetailBean.class);
            return DwsTransOrgDeliverSucDayBean.builder()
                    .districtId(dwdTransDeliverSucDetailBean.getReceiverDistrictId())
                    .cityId(dwdTransDeliverSucDetailBean.getReceiverCityId())
                    .provinceId(dwdTransDeliverSucDetailBean.getReceiverProvinceId())
                    .deliverSucCountBase(1L)
                    .ts(dwdTransDeliverSucDetailBean.getTs() + 8 * 60 * 60 * 1000L)
                    .build();
        });

        // TODO 4. 获取维度信息
        // 获取机构 ID
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withOrgIdStream = AsyncDataStream.unorderedWait(
                mappedStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj)  {
                        bean.setOrgId(dimJsonObj.getString("id"));
                    }

                    @Override
                    public Tuple2<String, String> getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return Tuple2.of("region_id", bean.getDistrictId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_org_id_stream");

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withWatermarkStream = withOrgIdStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTransOrgDeliverSucDayBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTransOrgDeliverSucDayBean>() {
                            @Override
                            public long extractTimestamp(DwsTransOrgDeliverSucDayBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                        .withIdleness(Duration.ofSeconds(20))
        ).uid("watermark_stream");

        // TODO 6. 按照机构 ID 分组
        KeyedStream<DwsTransOrgDeliverSucDayBean, String> keyedStream = withWatermarkStream.keyBy(DwsTransOrgDeliverSucDayBean::getOrgId);

        // TODO 7. 开窗
        WindowedStream<DwsTransOrgDeliverSucDayBean, String, TimeWindow> windowStream =
                keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1L)));

        // TODO 8. 引入触发器
        WindowedStream<DwsTransOrgDeliverSucDayBean, String, TimeWindow> triggerStream = windowStream.trigger(new MyTriggerFunction<>());

        // TODO 9. 聚合
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> aggregatedStream = triggerStream.aggregate(
                new MyAggregationFunction<DwsTransOrgDeliverSucDayBean>() {
                    @Override
                    public DwsTransOrgDeliverSucDayBean add(DwsTransOrgDeliverSucDayBean value, DwsTransOrgDeliverSucDayBean accumulator) {
                        if (accumulator == null) {
                            return value;
                        }
                        accumulator.setDeliverSucCountBase(
                                accumulator.getDeliverSucCountBase() + value.getDeliverSucCountBase()
                        );
                        return accumulator;
                    }
                },
                new ProcessWindowFunction<DwsTransOrgDeliverSucDayBean, DwsTransOrgDeliverSucDayBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<DwsTransOrgDeliverSucDayBean> elements, Collector<DwsTransOrgDeliverSucDayBean> out) throws Exception {
                        for (DwsTransOrgDeliverSucDayBean element : elements) {
                            long stt = context.window().getStart();
                            element.setCurDate(DateFormatUtil.toDate(stt - 8 * 60 * 60 * 1000L));
                            element.setTs(System.currentTimeMillis());
                            out.collect(element);
                        }
                    }
                }
        ).uid("aggregate_stream");

        // TODO 10. 补全维度信息
        // 10.1 补充机构名称
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withOrgNameAndRegionIdStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_organ") {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj){
                        bean.setOrgName(dimJsonObj.getString("org_name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return Tuple2.of("id",bean.getOrgId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_org_name_and_region_id_stream");

        // 10.2 补充城市名称
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> withCityNameStream = AsyncDataStream.unorderedWait(
                withOrgNameAndRegionIdStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj) {
                        bean.setCityName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return Tuple2.of("id",bean.getCityId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_city_name_stream");

        // 11.3 补充省份名称
        SingleOutputStreamOperator<DwsTransOrgDeliverSucDayBean> fullStream = AsyncDataStream.unorderedWait(
                withCityNameStream,
                new DimAsyncFunction<DwsTransOrgDeliverSucDayBean>("dim_base_region_info") {
                    @Override
                    public void join(DwsTransOrgDeliverSucDayBean bean, JSONObject dimJsonObj) {
                        bean.setProvinceName(dimJsonObj.getString("name"));
                    }

                    @Override
                    public Tuple2<String,String> getCondition(DwsTransOrgDeliverSucDayBean bean) {
                        return Tuple2.of("id",bean.getProvinceId());
                    }
                },
                60, TimeUnit.SECONDS
        ).uid("with_province_name_stream");

        // TODO 12. 写出到 ClickHouse
        fullStream.print(">>>");
        fullStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into dws_trans_org_deliver_suc_day_base values(?,?,?,?,?,?,?,?,?)")
        ).uid("clickhouse_stream");

        env.execute();
    }
}