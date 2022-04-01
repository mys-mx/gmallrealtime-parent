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


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String basePath = "hdfs://hadoop01:8020/";
        String tablePath = basePath.concat("warehouse/iceberg/sensordata");
        TableLoader tableLoader = TableLoader.fromHadoopTable(tablePath);

        DataStream<RowData> batchData = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(false)
                .build();

        batchData.print();


        env.execute("Flink  Iceberg  Select");
    }
}
