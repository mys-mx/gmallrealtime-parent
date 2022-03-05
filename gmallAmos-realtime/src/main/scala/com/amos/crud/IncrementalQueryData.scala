package com.amos.crud

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME, QUERY_TYPE, QUERY_TYPE_INCREMENTAL_OPT_VAL}
import org.apache.spark.sql.SparkSession

object IncrementalQueryData {

  /**
   * 增量方式查询数据，需要指定时间戳
   *
   * @param spark
   * @param path
   */
  def incrementalQueryData(spark: SparkSession, path: String): Unit = {
    import spark.implicits._
    //1.加载hudi表数据,获取commit time时间，作为增量查询数据阈值
    import org.apache.hudi.DataSourceWriteOptions._
    spark.read
      .format("hudi")
      .load(path)
      .createOrReplaceTempView("view_temp_hudi_trips")

    val commits: Array[String] = spark.sql(
      """
        |select
        |   distinct(_hoodie_commit_time) as commitTime
        |from view_temp_hudi_trips
        |order by
        |   commitTime desc
        |""".stripMargin
    ).map(row => row.getString(0))
      .take(50)

    val beginTime: String = commits(commits.length - 1)
    println($"beginTime = ${beginTime}")


    //2.设置hudi数据commitTime时间阈值，进行增量数据查询
    val tripsIncrementalDF = spark.read
      .format("hudi")
      .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(BEGIN_INSTANTTIME.key(), beginTime)
      .load(path)

    //3.将增量查询数据注册为临时试图，查询费用大于20的数据
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")
    spark.sql(
      """
        |select
        |  `_hoodie_commit_time`,fare,begin_lon,begin_lat,ts
        |from
        |  hudi_trips_incremental
        |where
        |  fare >20.0
        |""".stripMargin
    ).show(10, truncate = false)
  }

}
