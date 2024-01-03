package com.atguigu.tms.realtime.utils;

import com.atguigu.tms.realtime.beans.TransientSink;
import com.atguigu.tms.realtime.commom.TmsConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    // 获取SinkFunction
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        // 将流中对象的属性给问号占位符赋值
                        // 获取单签流中对象岁数的类型 以及类中的属性
                        Field[] fieldsArr = obj.getClass().getDeclaredFields();
                        // 遍历所有属性
                        int skipNum = 0;
                        for (int i = 0; i < fieldsArr.length; i++) {
                            Field field = fieldsArr[i];
                            // 判断当前属性是否需要向流中保存
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                skipNum++;
                                continue;
                            }
                            // 设置私有属性的访问权限
                            field.setAccessible(true);
                            try {
                                Object fieldValue = field.get(obj);
                                ps.setObject(i + 1 - skipNum, fieldValue);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5000)
                        .withBatchIntervalMs(3000L)
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(TmsConfig.CLICKHOUSE_DRIVER)
                        .withUrl(TmsConfig.CLICKHOUSE_URL)
                        .build()
        );
        return sinkFunction;
    }
}
