package com.amos

import org.apache.spark.sql.SparkSession


/**
 * SparkSQL 与 Iceberg整合
 */
object SparkIceberg_DDL {
  def main(args: Array[String]): Unit = {

    //创建Spark Catalog
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("SparkOperateIceberg")
      // 设置 hive catalog
      //      .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
      //      .config("spark.sql.catalog.hive_prod.type", "hive")
      // 元数据的链接url;默认在 hive-site.xml 中配置
      //      .config("spark.sql.catalog.hive_prod.uri", "thrift://hadoop01:9083")
      //      .config("iceberg.engine.hive.enabled", "true")

      //设置hadoop catalog
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://hadoop01:8020/sparkoperateiceberg")
      .getOrCreate()

    // 创建普通iceberg表
    //    spark.sql(
    //      """
    //        |create table if not exists
    //        |hadoop_prod.default.test_hadoop(
    //        | id int,
    //        | name string,
    //        | age int )
    //        |using iceberg
    //        |""".stripMargin)


    //创建普通分区表
    //    spark.sql(
    //      """
    //        |create table if not exists
    //        |hadoop_prod.default.test_hadoop_dt(
    //        | id int,
    //        | name string,
    //        | dt int )
    //        |using iceberg
    //        |partitioned by (dt)
    //        |""".stripMargin)

    spark.sql(
      """
        | drop table if exists hadoop_prod.default.test_hadoop_dt_hidden1
        |""".stripMargin)
    //创建隐藏分区表
    spark.sql(
      """
        |create table if not exists
        |hadoop_prod.default.test_hadoop_dt_hidden1(
        | id int,
        | name string,
        | dt int,
        | ts timestamp)
        |using iceberg
        |partitioned by (dt,hours(ts))
        |""".stripMargin)

    spark.stop()
  }

}