package com.amos.crud

import org.apache.spark.sql.{SaveMode, SparkSession}

object InsertData {
  def insertData(spark: SparkSession, table: String, path: String): Unit = {

    import spark.implicits._

    // step1：模拟乘车数据
    import org.apache.hudi.QuickstartUtils._
    val dataGen: DataGenerator = new DataGenerator()
    val inserts = convertToStringList(dataGen.generateInserts(100))

    import scala.collection.JavaConverters._

    val insertDF = spark.read.json(
      spark.sparkContext.parallelize(inserts.asScala, 2).toDS()
    )

    /* insertDF.printSchema()
   insertDF.show(10, truncate = false)*/

    //step2：插入数据到hudi中
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._


    insertDF.write
      .mode(SaveMode.Append)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      //hudi表的属性设置
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), table)
      .save(path)
  }
}
