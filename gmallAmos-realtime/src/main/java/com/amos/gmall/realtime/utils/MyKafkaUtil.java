package com.amos.gmall.realtime.utils;

import com.amos.gmall.realtime.common.CommonPropertiesConstants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2021-08-17 20:40
 */
public class MyKafkaUtil {

    private static final String KAFKA_SERVER = CommonPropertiesConstants.KAFKA_BROKERS;
    private static final String DEFAULT_TOPIC = CommonPropertiesConstants.DEFAULT_DATA;


    // TODO 获取flinkkafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String group) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
        // stringFlinkKafkaConsumer.setStartFromEarliest();
        return stringFlinkKafkaConsumer;
    }

    //todo 封装flink生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        //SimpleStringSchema对字符串序列化
        return new FlinkKafkaProducer<String>(KAFKA_SERVER, topic, new SimpleStringSchema());

    }


    //todo 封装flink生产者
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {


        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //指定生产数据超时时间
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }

}
