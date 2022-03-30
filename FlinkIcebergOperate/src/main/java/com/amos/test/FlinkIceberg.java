package com.amos.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

import java.util.Map;


public class FlinkIceberg {

    public static void main(String[] args) throws Exception {

        // 创建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint
        env.enableCheckpointing(5000);

        //Flink读取kafka中的数据配置
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop01:9092,hadoop02:9092,hadoop03:9092")
                .setTopics("flink-iceberg-topic")
                .setGroupId("my-group-id")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        //读取kafka中的数据
        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

        SingleOutputStreamOperator<RowData> kafkaDS = dataStreamSource.map(new MapFunction<String, RowData>() {
            @Override
            public RowData map(String line) throws Exception {
                String[] split = line.split(",");
                GenericRowData rowData = new GenericRowData(4);
                rowData.setField(0, Integer.valueOf(split[0]));
                rowData.setField(1, StringData.fromString(split[1]));
                rowData.setField(2, Integer.valueOf(split[2]));
                rowData.setField(3, StringData.fromString(split[3]));
                return rowData;
            }
        });

        //Flink 创建iceberg表
        Configuration hadoopConf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "hdfs://hadoop01:8020/flink_iceberg");

        //配置iceberg 库名和表名
        TableIdentifier name = TableIdentifier.of("icebergdb", "flink_iceberg_tb1");

        //创建Iceberg表Schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.StringType.get()),
                Types.NestedField.required(4, "loc", Types.StringType.get()));

        //如果有分区制定对应分区，这里'loc'列为分区列，可以制定unpartitioned方法不设置分区
        // PartitionSpec spec = PartitionSpec.unpartitioned();
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("loc").build();

        // 指定Iceberg表数据格式化为Parquet存储
        Map<String, String> prop = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());

        Table table = null;

        //通过catalog判断表是否存在，不存在就创建，存在就加载
        if (!catalog.tableExists(name)) {
            table = catalog.createTable(name, schema, spec, prop);

        } else {
            table = catalog.loadTable(name);
        }

        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://hadoop01:8020/flink_iceberg/icebergdb/flink_iceberg_tb1", hadoopConf);

        //将流式结果写出Iceberg表中
        FlinkSink.forRowData(kafkaDS)
                .table(table)//可写可不写
                .tableLoader(tableLoader)
                .overwrite(false)//false不覆盖数据
                .build();

        env.execute("DataStream API Write Iceberg Table");

    }


}


















