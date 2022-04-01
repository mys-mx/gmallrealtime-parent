package com.amos

import org.apache.spark.sql.SparkSession

object SparkIceberg_Delete {
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


    spark.sql(
      """
        |delete from hadoop_prod.default.test_hadoop_dt_hidden1
        |where id in (1,2,3,4,5,6)
        |""".stripMargin)
    spark.sql(
      """
        |select *  from hadoop_prod.default.test_hadoop_dt_hidden1
        |""".stripMargin).show()

    spark.stop()
  }

}
