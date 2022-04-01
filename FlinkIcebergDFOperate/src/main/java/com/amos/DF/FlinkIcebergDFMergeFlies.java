package com.amos.DF;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.actions.Actions;

import java.util.HashMap;

/**
 * @program: gmallrealtime-parent
 * @description: flink sql合并小文件
 * @create: 2022-04-01 12:53
 */
public class FlinkIcebergDFMergeFlies {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        //使用hadoopCatalog的模式，获取Iceberg表
        HashMap<String, String> icebergMap = new HashMap<>();
        icebergMap.put("type", "iceberg");
        icebergMap.put("catalog-type", "hadoop");
        icebergMap.put("property-version", "1");
        icebergMap.put("warehouse", "hdfs://hadoop-slave2:6020/sparkoperateiceberg");
        CatalogLoader hadoopCatalog = CatalogLoader.hadoop("hadoop", new Configuration(), icebergMap);
        Catalog catalog = hadoopCatalog.loadCatalog();

        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of("default"), "test_hadoop_dt_hidden");
        Table table = catalog.loadTable(tableIdentifier);


        //执行合并操作，快照过期
        combineFiles(env, table);
        deleteOldSnapshot(table);

//        env.execute("flink dataframe merge  small files");
    }

    //合并小文件
    private static void combineFiles(StreamExecutionEnvironment env, Table table) {
        //合并小的数据文件
        Actions.forTable(env, table).rewriteDataFiles()
                .maxParallelism(1)
                .targetSizeInBytes(128 * 1024 * 1024)
                .execute();

        //重写manifest文件
        table.rewriteManifests()
                .rewriteIf((file) -> file.length() < 32 * 1024 * 1024)
                .clusterBy((file) -> file.partition().get(0, Integer.class))
                .commit();
    }

    // 删除过期快照
    private static void deleteOldSnapshot(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        long oldSnapshot = snapshot.timestampMillis();
        if (snapshot != null) {
            table.expireSnapshots().expireOlderThan(oldSnapshot).commit();
        }
    }
}
