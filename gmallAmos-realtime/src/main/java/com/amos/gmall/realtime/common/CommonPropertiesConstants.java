package com.amos.gmall.realtime.common;


/**
 * @date 2021/4/18 17:24
 */
public class CommonPropertiesConstants {
    /**
     * Kafka 消费组id
     */
    public static final String PROPERTIES_FILE_NAME ="/common.properties";


    /**
     * Kafka 消费组id
     */
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    /**
     * Kafka 集群地址
     */
    public static final String KAFKA_BROKERS = "kafka.brokers";

    /**
     * 默认数据发送kafka 的topic
     */
    public static final String DEFAULT_DATA = "default.data.topic";
    /**
     * 状态后端集群地址
     */
    public static final String FS_STATE_BACKEND_ADDRESS = "fs.state.backend.address";


}
