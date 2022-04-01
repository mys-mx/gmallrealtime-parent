package com.amos.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: gmallrealtime-parent
 * @description: flink 1.11.6 和 iceberg 0.11.1 创建表和插入数据
 * @create: 2022-03-31 16:30
 */
public class FlinkSQLCreateInsert {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("pipeline.name", FlinkSQLCreateInsert.class.getName());

        env.enableCheckpointing(1000);
        //1.设置catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with ('type'='iceberg'," +
                "'catalog-type'='hadoop','warehouse'='hdfs://hadoop-slave2:6020/flink_iceberg')");

        //2.使用当前catalog
        tableEnv.useCatalog("hadoop_iceberg");

        //3.创建数据
        tableEnv.executeSql("create database if not exists iceberg_db");

        //4.使用当前库
        tableEnv.useDatabase("iceberg_db");

        //5.创建Iceberg表
        tableEnv.executeSql("create table  hadoop_iceberg.iceberg_db.flink_iceberg_sql(id int,name string,age int,loc string) partitioned by (loc)");

        //6.向表中插入数据
        tableEnv.executeSql("insert into hadoop_iceberg.iceberg_db.flink_iceberg_sql values (1,'zs',18,'shanghai'),(2,'ls',19,'beijing'),(3,'lm',20,'shanghai')");


    }
}
