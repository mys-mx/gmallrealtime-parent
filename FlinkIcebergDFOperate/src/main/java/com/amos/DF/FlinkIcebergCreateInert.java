package com.amos.DF;

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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.Map;

/**
 * @program: gmallrealtime-parent
 * @description:
 * @create: 2022-03-31 13:23
 */
public class FlinkIcebergCreateInert {

    // 4 Hadoop集群
    private static final String basePath = "hdfs://hadoop-slave2:6020/warehouse/iceberg/";
    private static final String tablePath = basePath.concat("hadoop/flink_iceberg_df");
    private static final String BOOTS_SERVER = "node187:9092,hadoop-slave2:9092,node186:9092";
    private static final String TOPIC = "flink-iceberg-topic";
    private static final String GROUPID = "flink-group-id1";
    private static final String SOURCE_NAME = "kafka_source";

    public static void main(String[] args) throws Exception {
        // 1 设置Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置checkpoint
        env.enableCheckpointing(5000);
        //Flink读取kafka中的数据配置
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTS_SERVER)
                .setTopics(TOPIC)
                .setGroupId(GROUPID)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        //加载kafka中数据
        DataStreamSource<String> inputStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), SOURCE_NAME);
        //数据转换
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

        // 通过flink sql创建iceberg表
        Configuration conf = new Configuration();
        HadoopCatalog hpCatalog = new HadoopCatalog(conf, basePath);
        TableIdentifier name = TableIdentifier.of("hadoop", "flink_iceberg_df");
        // 不设置分区
        PartitionSpec spec = PartitionSpec.unpartitioned();
        // 构建schemal信息
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get()),
                Types.NestedField.required(4, "loc", Types.StringType.get()));
        Map<String, String> props =
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
        //表是否存在 ，不存在创建
        if (!hpCatalog.tableExists(name)) {
            hpCatalog.createTable(name, schema, spec, props);
        }
        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath, conf);

        FlinkSink.forRowData(kafkaDS)
                .tableLoader(tableLoader)
                .overwrite(false)//false不覆盖数据
                .build();
        env.execute(" flink 向 Iceberg 中写入数据");
    }
}
