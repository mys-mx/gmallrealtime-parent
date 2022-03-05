package com.amos.crud

import org.apache.spark.sql.SparkSession

object QueryData{
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
      .select($"uuid",$"driver", $"rider", $"fare", $"end_lat",
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
}
