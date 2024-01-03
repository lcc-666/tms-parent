package com.atguigu.tms.realtime.commom;

// 物流实时数仓常量类
public class TmsConfig {
    // zk的地址
    public static final String hbase_zookeeper_quorum="hadoop102,hadoop103,hadoop104";
    // hbase的namespace
    public static final String HBASE_NAMESPACE="tms_realtime";

    // clickhouse驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    // clickhouseURL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/tms_realtime";
}
