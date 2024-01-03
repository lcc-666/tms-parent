package com.atguigu.tms.realtime.app.dim;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.tms.realtime.app.func.DimSinkFunction;
import com.atguigu.tms.realtime.app.func.MyBroadcastProcessFunction;
import com.atguigu.tms.realtime.beans.TmsConfigDimBean;
import com.atguigu.tms.realtime.commom.TmsConfig;
import com.atguigu.tms.realtime.utils.CreateEnvUtil;
import com.atguigu.tms.realtime.utils.HbaseUtil;
import com.atguigu.tms.realtime.utils.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class DimApp {
    public static void main(String[] args) throws Exception {
        // 1.基本环境准备
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(args);
        env.setParallelism(4);

        // 2.从Kafka的tms_ods主题中读取业务数据
        String topic = "tms_ods";
        String groupID = "dim_app_group";

        KafkaSource<String> kafkaSource = KafkaUtil.getKafkaSource(topic, groupID, args);

        SingleOutputStreamOperator<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .uid("kafka source");

//        kafkaStrDS.print(">>>");

        // 3.对读取的数据进行类型转换并过滤掉不需要传递json属性
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(
                (MapFunction<String, JSONObject>) jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);

                    String table = jsonObj.getJSONObject("source").getString("table");
                    jsonObj.put("table", table);
//                    jsonObj.remove("before");
                    jsonObj.remove("source");
                    jsonObj.remove("transaction");
                    return jsonObj;
                }
        );
        // 4.使用FlinkCDC读取配置表数据
        MySqlSource<String> mysqlSource = CreateEnvUtil.getMysqlSource("config_dim", "6000", args);

        SingleOutputStreamOperator<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1)
                .uid("mysql_source");


        // 5.提前将hbase中的维度表创建出来
        SingleOutputStreamOperator<String> createTableDS = mysqlDS.map(
                (MapFunction<String, String>) jsonStr -> {
                    // 将jsonStr转换成jsonObj
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    // 获取对配置表的操作
                    String op = jsonObj.getString("op");
                    if ("r".equals(op) || "c".equals(op)) {
                        // 获取after属性的值
                        JSONObject afterObject = jsonObj.getJSONObject("after");
                        String sinkTable = afterObject.getString("sink_table");
                        String sinkFamily = afterObject.getString("sink_family");
                        if (StringUtils.isEmpty(sinkFamily)) {
                            sinkFamily = "info";
                        }
                        System.out.println("在Hbase中创建表：" + sinkTable);
                        HbaseUtil.createTable(TmsConfig.HBASE_NAMESPACE, sinkTable, sinkFamily.split(","));

                    }
                    return jsonStr;
                });



        // 6.对配置数据进行广播
        MapStateDescriptor<String, TmsConfigDimBean> mapStateDescriptor
                = new MapStateDescriptor<>("mapStateDescriptor", String.class, TmsConfigDimBean.class);
        BroadcastStream<String> broadcastDS = createTableDS.broadcast(mapStateDescriptor);


        // 7.将主流和广播流进行关联--connect
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjDS.connect(broadcastDS);



        // 8.对关联之后的数据进行处理
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(
                new MyBroadcastProcessFunction(mapStateDescriptor, args)
        );

        // 9.将数据保存到Hbase中
        dimDS.print(">>>");
        dimDS.addSink(new DimSinkFunction());


        env.execute();
    }
}
