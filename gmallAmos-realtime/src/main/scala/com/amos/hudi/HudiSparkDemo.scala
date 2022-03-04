package com.amos.hudi

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 *
 */

object HudiSparkDemo {

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

  /**
   * 采用 Snapshot Query快照方式查询表的数据
   *
   * @param spark
   * @param path
   */
  def queryData(spark: SparkSession, path: String) = {

    import spark.implicits._
    val tripsDF = spark.read.format("hudi")
      .load(path)
    //    tripsDF.printSchema()
    //    tripsDF.show(10, truncate = false)
    //查询费用大于20，小于50的乘车数据
    tripsDF.filter($"fare" >= 20 && $"fare" < 50)
      .select($"driver", $"rider", $"fare", $"end_lat",
        $"end_lon", $"partitionpath", $"_hoodie_commit_time")
      .orderBy($"fare".desc, $"_hoodie_commit_time".desc)
      .show(20, truncate = false)

  }

  def queryDataByTime(spark: SparkSession, path: String) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val tripsDF = spark.read
      .format("hudi")
      .option("as.of.instant", "20220304222958")
      .load(path)
      .sort($"_hoodie_commit_time".desc)
    tripsDF.printSchema()
    tripsDF.show(10, truncate = false)


    val tripsDF1 = spark.read
      .format("hudi")
      .option("as.of.instant", "2022-03-04 22:29:58")
      .load(path)
      .sort($"_hoodie_commit_time".desc)
    tripsDF1.printSchema()
    tripsDF1.show(10, truncate = false)

  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }

    //定义变量:表名称、保存路径
    val tableName: String = "tbl_trips_cow"
    val tablePath: String = "/hudi-warehouse/tbl_trips_cow"

    //构建数据生成器，模拟产生业务数据
    import org.apache.hudi.QuickstartUtils._


    // 任务一：模拟数据，插入Hudi表，采用cow模式
    //    insertData(spark, tableName, tablePath)


    //任务二：快照方式查询（snapshot Query）数据，采用DSL方式
    //    queryData(spark, tablePath)
    queryDataByTime(spark, tablePath)

    //应用结束，关闭资源
    spark.stop()
  }

}
