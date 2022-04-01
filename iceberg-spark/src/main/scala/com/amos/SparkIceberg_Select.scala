package com.amos

import org.apache.spark.sql.SparkSession

object SparkIceberg_Select {
  def main(args: Array[String]): Unit = {
    //创建Spark Catalog
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkOperateIceberg")
      //设置hadoop catalog
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://hadoop-slave2:6020/sparkoperateiceberg")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    //    spark.sql(
    //      """
    //        |select * from hadoop_prod.default.test_hadoop_dt_hidden
    //        |where dt=21
    //        |""".stripMargin).show()

    //    spark.sql(
    //      """
    //        |select * from hadoop_prod.default.test_hadoop_dt_hidden1
    //        |where dt=21
    //        |""".stripMargin).show()

    //查看表快照
//    spark.sql(
//      """
//        |select * from hadoop_prod.default.test_hadoop_dt.snapshots
//        |""".stripMargin).show(false)
    //查看表历史
//        spark.sql(
//          """
//            |select * from hadoop_prod.default.test_hadoop_dt.history
//            |""".stripMargin).show()
    //查看表data files
    //    spark.sql(
    //      """
    //        |select * from hadoop_prod.default.test_hadoop_dt_hidden.files
    //        |""".stripMargin).show()

    //查看表manifests
    //    spark.sql(
    //      """
    //        |select * from hadoop_prod.default.test_hadoop_dt_hidden.manifests
    //        |""".stripMargin).show()

    //dataframe 模式进行快照查询 3242306923457299938
    //    spark.read.option("snapshot-id", 4548184957513312181L)
    //      .format("iceberg")
    //      .load("hdfs://hadoop01:8020/sparkoperateiceberg/default/test_hadoop_dt_hidden1")
    //      .show()

    //spark3.x 还可以指定sql查询 3242306923457299938
    spark.sql(
      """
        |CALL hadoop_prod.system.set_current_snapshot("default.test_hadoop_dt_hidden",  6100848443512088913 )
        |""".stripMargin)
    spark.sql(
      """
        |select * from hadoop_prod.default.test_hadoop_dt_hidden
        |""".stripMargin).show()

    spark.stop()
  }

}
