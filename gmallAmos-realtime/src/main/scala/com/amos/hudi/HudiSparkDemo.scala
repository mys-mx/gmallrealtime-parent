package com.amos.hudi

import com.amos.crud.QueryData.queryDataByTime
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 *
 */

object HudiSparkDemo {

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
