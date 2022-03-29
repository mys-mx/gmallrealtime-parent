package com.amos

import org.apache.spark.sql.SparkSession

object SparkIceberg_Insert {
  def main(args: Array[String]): Unit = {
    //创建Spark Catalog
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkOperateIceberg")
      //设置hadoop catalog
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://hadoop01:8020/sparkoperateiceberg")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()

    //插入到普通iceberg表中
    //    spark.sql(
    //      """
    //        |insert into hadoop_prod.default.test_hadoop
    //        |values (1,"张三",20),(2,"李四",21)
    //        |""".stripMargin)

    // 插入到普通分区表中
    //    spark.sql(
    //      """
    //        |insert into hadoop_prod.default.test_hadoop_dt
    //        |values (1,"张三",21),(2,"李四",21)
    //        |""".stripMargin)

    // 插入到隐藏分区表中
    spark.sql(
      """
        |insert into hadoop_prod.default.test_hadoop_dt_hidden1
        |values
        |(1,"张居正",22,cast(from_unixtime(1638254119) as timestamp)),
        |(2,"杜甫",22,cast(from_unixtime(1648177719) as timestamp))
        |""".stripMargin)

    //update
    //    spark.sql(
    //      """
    //        |update hadoop_prod.default.test_hadoop_dt_hidden set name="张飞" where id=1
    //        |""".stripMargin)


    //merge into
    spark.sql(
      """
        |merge into hadoop_prod.default.test_hadoop_dt_hidden t
        |using (select * from hadoop_prod.default.test_hadoop_dt_hidden1) s
        |on t.id=s.id
        |WHEN MATCHED  THEN UPDATE SET *
        |when not matched then insert *
        |""".stripMargin)

    spark.sql(
      """
        |select * from hadoop_prod.default.test_hadoop_dt_hidden1
        |
        |""".stripMargin).show()

    spark.sql(
      """
        |select * from hadoop_prod.default.test_hadoop_dt_hidden
        |
        |""".stripMargin).show()

    spark.stop()
  }
}
