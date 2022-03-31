package com.amos.test;


import com.google.common.collect.ImmutableMap;
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
import org.apache.iceberg.*;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import java.util.Map;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2022-03-31 13:23
 */
public class FlinkIcebergHive {
    public static void main(String[] args) throws Exception {
        // 1 设置Flink执行环境
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

        // 2 加载数据源
        DataStreamSource<String> inputStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");


        SingleOutputStreamOperator<RowData> kafkaDS = inputStream.map(new MapFunction<String, RowData>() {
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
        PartitionSpec spec = PartitionSpec.unpartitioned();

        // 4 Hadoop集群
        String basePath = "hdfs://hadoop01:8020/";
        String tablePath = basePath.concat("warehouse/iceberg/sensordata");

        // 5 设置数据的存储格式：orc、parquet或者avro
        Map<String, String> props =
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());

        // 3 设置Hive表的Schema信息
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "loc", Types.StringType.get()));
        Table table = new HadoopTables().create(schema, spec, props, tablePath);

        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

        // 6 向Iceberg的hdfs的目录写入数据
        FlinkSink.forRowData(kafkaDS)
                .table(table)
                .tableLoader(tableLoader)
                .overwrite(false)//false不覆盖数据
                .build();

        env.execute("通过Append方式向Iceberg中写入数据");
    }
}
