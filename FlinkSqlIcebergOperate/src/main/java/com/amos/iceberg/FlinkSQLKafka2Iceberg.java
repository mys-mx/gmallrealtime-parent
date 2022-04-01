package com.amos.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: gmallrealtime-parent
 * @description: flink 实时读取kafka数据 写入到 iceberg
 * @create: 2022-04-01 11:13
 */
public class FlinkSQLKafka2Iceberg {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", FlinkSQLCreateInsert.class.getName());
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // todo 支持SQL语法中的OPTIONS选项
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        env.enableCheckpointing(5000);

        //1.设置catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with ('type'='iceberg'," +
                "'catalog-type'='hadoop','warehouse'='hdfs://hadoop-slave2:6020/flink_iceberg')");


        //2.读取kafka中数据
        tableEnv.executeSql("create table kafka_input_table(" +
                "id int, " +
                "name varchar, " +
                "age int ," +
                "loc varchar " +
                ") with ("+
                "'connector'='kafka',"+
                "'topic'='flink-iceberg-topic'," +
                "'properties.bootstrap.servers'='node187:9092,hadoop-slave2:9092,node186:9092'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='csv'," +
                "'properties.group.id'='my-group-id')"
        );

        //3.向iceberg中写数据
        tableEnv.executeSql("insert into  hadoop_iceberg.iceberg_db.flink_iceberg_sql1 select id,name,age,loc from kafka_input_table");

        //5.查询iceberg表数据并打印
        TableResult tableResult = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_sql1 /*+ OPTIONS('streaming'='true','monitor-interval'='1s')*/");
        tableResult.print();


    }
}
