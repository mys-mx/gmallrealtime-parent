package com.amos.DF;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program: gmallrealtime-parent
 * @description: flink sql合并小文件
 * @create: 2022-04-01 12:53
 */
public class FlinkIcebergDFMergeFlies {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);








        env.execute("flink dataframe merge  small files");
    }
}
