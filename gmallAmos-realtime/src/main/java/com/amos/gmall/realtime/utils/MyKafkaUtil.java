package com.amos.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2021-08-17 20:40
 */
public class MyKafkaUtil {
    private static String kafkaServer = "hadoop01:9092,hadoop02:9092,hadoop03:9092";


    // TODO 获取flinkkafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String group) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
//        stringFlinkKafkaConsumer.setStartFromEarliest();
        return stringFlinkKafkaConsumer;
    }

    //todo 封装flink生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {

        return new FlinkKafkaProducer<String>(kafkaServer, topic, new SimpleStringSchema());

    }


}
