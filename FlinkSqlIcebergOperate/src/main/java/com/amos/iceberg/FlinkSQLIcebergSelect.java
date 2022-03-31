package com.amos.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: gmallrealtime-parent
 * @description:flink 1.11.6 和 iceberg 0.11.1
 * @create: 2022-03-31 17:40
 */
public class FlinkSQLIcebergSelect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", FlinkSQLCreateInsert.class.getName());
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // todo 支持SQL语法中的OPTIONS选项
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        env.enableCheckpointing(1000);

        //1.设置catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with ('type'='iceberg'," +
                "'catalog-type'='hadoop','warehouse'='hdfs://hadoop01:8020/flink_iceberg')");

        //2.批量读取表数据
        TableResult tableResult = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_sql");
        tableResult.print();


        //3.实时读取iceberg表数据
        TableResult tableResult1 = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_sql /*+ OPTIONS('streaming'='true','monitor-interval'='1s')*/");
        tableResult1.print();

    }
}
