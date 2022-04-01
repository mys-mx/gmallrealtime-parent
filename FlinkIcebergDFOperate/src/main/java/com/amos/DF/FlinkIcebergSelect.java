package com.amos.DF;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

/**
 * @program: gmallrealtime-parent
 * @description: flink 读取 iceberg的数据
 * @create: 2022-03-31 13:51
 */
public class FlinkIcebergSelect {
    private static final String basePath = "hdfs://hadoop-slave2:6020/warehouse/iceberg/";
    private static final String tablePath = basePath.concat("hadoop/flink_iceberg_df");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);
        DataStream<RowData> batchData = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();

        batchData.print();
        env.execute("Flink  Iceberg  Select");
    }
}
