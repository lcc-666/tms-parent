package com.atguigu.tms.realtime.utils;


import com.esotericsoftware.minlog.Log;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;

public class CreateEnvUtil {
    public static StreamExecutionEnvironment getStreamEnv(String[] args) {
        // 1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.检查点相关设置
        // 2.1 开启检查点
        env.enableCheckpointing(6000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.2 设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(120000L);
        // 2.3 设置job取消之后 检查点是否保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 2.4 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        // 2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(1), Time.seconds(3)));
        // 2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/tms/ck");
        // 2.7 设置操作hdfs用户
        // 获取命令行参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hdfsUserName = parameterTool.get("hadoop-user-name", "atguigu");
        System.setProperty("HADOOP_USER_NAME", hdfsUserName);
        return env;

    }

    public static MySqlSource<String> getMysqlSource(String option, String serverId, String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String mysqlHostname = parameterTool.get("hadoop-user-name", "hadoop102");
        int mysqlPort = Integer.parseInt(parameterTool.get("mysql-port", "3306"));
        String mysqlUsername = parameterTool.get("mysql-username", "root");
        String mysqlPasswd = parameterTool.get("mysql-passwd", "000000");
        option = parameterTool.get("start-up-option", option);
        serverId = parameterTool.get("server-id", serverId);

        // 创建配置信息 Map 集合，将 Decimal 数据类型的解析格式配置 k-v 置于其中
        HashMap config = new HashMap<>();
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        // 将前述 Map 集合中的配置信息传递给 JSON 解析 Schema，该 Schema 将用于 MysqlSource 的初始化
        JsonDebeziumDeserializationSchema jsonDebeziumDeserializationSchema =
                new JsonDebeziumDeserializationSchema(false, config);

        MySqlSourceBuilder<String> builder = MySqlSource.<String>builder()
                .hostname(mysqlHostname)
                .port(mysqlPort)
                .username(mysqlUsername)
                .password(mysqlPasswd)
                .deserializer(jsonDebeziumDeserializationSchema);
        switch (option) {
            // 读取事实数据
            case "dwd":
                String[] dwdTables = new String[]{
                        "tms.order_info",
                        "tms.order_cargo",
                        "tms.transport_task",
                        "tms.order_org_bound"};
                return builder
                        .databaseList("tms")
                        .tableList(dwdTables)
                        .startupOptions(StartupOptions.latest())
                        .serverId(serverId)
                        .build();

            // 读取维度数据
            case "realtime_dim":
                String[] realtimeDimTables = new String[]{
                        "tms.user_info",
                        "tms.user_address",
                        "tms.base_complex",
                        "tms.base_dic",
                        "tms.base_region_info",
                        "tms.base_organ",
                        "tms.express_courier",
                        "tms.express_courier_complex",
                        "tms.employee_info",
                        "tms.line_base_shift",
                        "tms.line_base_info",
                        "tms.truck_driver",
                        "tms.truck_info",
                        "tms.truck_model",
                        "tms.truck_team"};
                return builder
                        .databaseList("tms")
                        .tableList(realtimeDimTables)
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();
            case "config_dim":
                return builder
                        .databaseList("tms_config")
                        .tableList("tms_config.tms_config_dim")
                        .startupOptions(StartupOptions.initial())
                        .serverId(serverId)
                        .build();



        }

        Log.error("不支持操作类型");
        return null;

    }
}
