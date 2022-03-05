package com.amos.crud

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 删除Hudi表数据，依据主键UUID进行删除，如果是分区表指定分区路径进行删除
 */
object DeleteData {
  def deleteData(spark: SparkSession, table: String, path: String): Unit = {
    import spark.implicits._

    //1.加载hudi表数据，获取条目数
    val tripsDF: DataFrame = spark.read
      .format("hudi")
      .load(path)
    println(s"Row Count = ${tripsDF.count()}")

    //2.模拟要删除的数据，从hudi表中加载数据，获取几条数据，转换为要删除的数据集合
    val dataFrame = tripsDF.limit(2).select($"uuid", $"partitionpath")
    import org.apache.hudi.QuickstartUtils._

    val dataGenerator = new DataGenerator()
    val deletes = dataGenerator.generateDeletes(dataFrame.collectAsList())

    import scala.collection.JavaConverters._
    val deleteDF = spark.read.json(spark.sparkContext.parallelize(deletes.asScala, 2))

    //3.保存数据到hudi表中，设置操作类型为DELETE
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._
    deleteDF.write
      .mode(SaveMode.Append)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", 2)
      .option("hoodie.upsert.shuffle.parallelism", 2)
      //设置数据操作类型为delete，默认值为upsert
      .option(OPERATION.key(), "delete")
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), table)
      .save(path)

    //4.再次加载hudi表数据，统计条目数，查看是否减少2条数据
    val verifyDF: DataFrame = spark.read
      .format("hudi")
      .load(path)
    println(s"After Delete Raw Count = ${verifyDF.count()}")

  }

}
