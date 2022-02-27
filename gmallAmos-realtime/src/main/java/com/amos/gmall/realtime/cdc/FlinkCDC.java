package com.amos.gmall.realtime.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.amos.gmall.realtime.common.CommonPropertiesConstants;
import com.amos.gmall.realtime.common.MysqlPropertiesConstants;
import com.amos.gmall.realtime.common.PropertiesConstants;
import com.amos.gmall.realtime.utils.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;

/**
 * @program: gmallrealtime-parent
 * @description: flink-cdc 学习
 * @create: 2022-02-27 16:37
 */
public class FlinkCDC {

    /**
     * 参数路径
     */
    private static final String JOB_NAME = "FlinkCDC";
    private static final int CHECKPOINT_INTERVAL = 5000;
    private static final String MYSQL_PORT = MysqlPropertiesConstants.MYSQL_PORT;
    private static final String MYSQL_HOST = MysqlPropertiesConstants.MYSQL_HOST;
    private static final String MYSQL_USER = MysqlPropertiesConstants.MYSQL_USER;
    private static final String MYSQL_PASSWORD = MysqlPropertiesConstants.MYSQL_PASSWORD;
    private static final String MYSQL_DATABASE = MysqlPropertiesConstants.MYSQL_DATABASES;
    private static final String MYSQL_TABLES = MysqlPropertiesConstants.MYSQL_TABLES;
    private static final String CHECKPOINT_PATH = MysqlPropertiesConstants.FLINK_CDC_BACKEND_PATH;
    private static final String CDC_PARALLELISM = MysqlPropertiesConstants.CDC_PARALLELISM;

    /**
     * 参数配置路径
     */
    private static final String PROPERTIES_FILE_NAME = MysqlPropertiesConstants.MYSQL_PROPERTIES_FILE_NAME;

    private static final String FS_STATE_BACKEND_ADDRESS = CommonPropertiesConstants.FS_STATE_BACKEND_ADDRESS;

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterToolNew(args, PROPERTIES_FILE_NAME);


        //1. 获取执行环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.enableCheckpointing(CHECKPOINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(
                parameterTool.get(FS_STATE_BACKEND_ADDRESS)
                        + parameterTool.get(CHECKPOINT_PATH)
        ));
        env.setParallelism(parameterTool.getInt(CDC_PARALLELISM));

        //2.通过flink-cdc构建sourceFunction,并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(parameterTool.get(MYSQL_HOST))
                .username(parameterTool.get(MYSQL_USER))
                .password(parameterTool.get(MYSQL_PASSWORD))
                .port(parameterTool.getInt(MYSQL_PORT))
                //flink cdc可以同时读多个库
                .databaseList(parameterTool.get(MYSQL_DATABASE))
                //如果不传入参数则是监控该库下面所有的表，如果指定监控一个库下面的表需要写db.tableName
                .tableList(parameterTool.get(MYSQL_TABLES))
                .deserializer(new StringDebeziumDeserializationSchema())
                /**
                 * 1.initial是将历史数据全量加载(加锁)，然后再增量查binlog
                 * 2.earliest 不做初始化，从binlog开始的位置读
                 * 3.latest 只获取从链接开始之后的数据
                 * 4.specificOffset 从指定偏移量位置开始读
                 * 5.timestamp  从指定时间戳位置开始读
                 */
                .startupOptions(StartupOptions.initial())
                .build();


        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);
        //3.打印数据
        stringDataStreamSource.print();
        //4.启动任务
        env.execute(JOB_NAME);

    }
}
