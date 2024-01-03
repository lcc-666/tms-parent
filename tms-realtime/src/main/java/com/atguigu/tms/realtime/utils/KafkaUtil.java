package com.atguigu.tms.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;

public class KafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static KafkaSink<String> getKafkaSink(String topic, String transIdPrefix, String[] args) {
        // 将命令行参数对象封装为 ParameterTool 类对象
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 提取命令行传入的 key 为 topic 的配置信息，并将默认值指定为方法参数 topic
        // 当命令行没有指定 topic 时，会采用默认值
        topic = parameterTool.get("topic", topic);
        // 如果命令行没有指定主题名称且默认值为 null 则抛出异常
        if (topic == null) {
            throw new IllegalArgumentException("主题名不可为空：命令行传参为空且没有默认值!");
        }

        // 获取命令行传入的 key 为 bootstrap-servers 的配置信息，并指定默认值
        String bootstrapServers = parameterTool.get("bootstrap-severs", KAFKA_SERVER);
        // 获取命令行传入的 key 为 transaction-timeout 的配置信息，并指定默认值
        String transactionTimeout = parameterTool.get("transaction-timeout", 15 * 60 * 1000 + "");


        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix(transIdPrefix)
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout)
                .build();

    }

    public static KafkaSink<String> getKafkaSink(String topic, String[] args) {
        return getKafkaSink(topic, topic + "_trans", args);

    }

    //获取KafkaSource
    public static KafkaSource<String> getKafkaSource(String topic,String groupID,String[] args){

        // 将命令行参数对象封装为 ParameterTool 类对象
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 提取命令行传入的 key 为 topic 的配置信息，并将默认值指定为方法参数 topic
        // 当命令行没有指定 topic 时，会采用默认值
        topic = parameterTool.get("topic", topic);
        // 如果命令行没有指定主题名称且默认值为 null 则抛出异常
        if (topic == null) {
            throw new IllegalArgumentException("主题名不可为空：命令行传参为空且没有默认值!");
        }

        // 获取命令行传入的 key 为 bootstrap-servers 的配置信息，并指定默认值
        String bootstrapServers = parameterTool.get("bootstrap-severs", KAFKA_SERVER);
        // 获取命令行传入的 key 为 transaction-timeout 的配置信息，并指定默认值

        return KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(topic)
                .setGroupId(groupID)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                // 注意SimpleStringSchema无法处理空值，需要自定义反序列化
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

    }
}
