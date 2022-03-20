package com.amos.hudi.didi

import com.amos.hudi.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
 * 滴滴海口出行运营数据分析，使用sparksql操作表数据，按照业务需求统计
 */
object DidiAnalysisSpark {

  val hudiTablePath: String = "/hudi-warehouse/tbl_didi_haikou"

  /**
   * 从hudi获取数据
   *
   * @param spark
   * @param hudiTablePath
   * @return
   */
  def readFromHudi(spark: SparkSession, hudiTablePath: String): DataFrame = {

    val didiDF = spark.read.format("hudi")
      .load(hudiTablePath)
    //获取字段
    didiDF.select(
      "product_id", "type", "traffic_type", "pre_total_fee", "start_dest_distance", "departure_time"
    )
  }

  /**
   * 订单类型统计 字段：product_id
   *
   * @param hudiDF
   */
  def reportProduct(hudiDF: DataFrame) = {
    // a. 按照产品线ID分组统计即可
    val reportDF: DataFrame = hudiDF.groupBy("product_id").count()

    //b.自定义udf函数转换名称
    val to_name = udf(
      (productID: Int) => {
        productID match {
          case 1 => "滴滴专车"
          case 2 => "滴滴企业专车"
          case 3 => "滴滴快车"
          case 4 => "滴滴企业快车"
        }
      }
    )

    //c.转换名称
    val resultDF: DataFrame = reportDF.select(
      to_name(col("product_id")).as("order_type"),
      col("count").as("total")
    )

    //    resultDF.printSchema()
    //    resultDF.show(10)


  }

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass)

    val hudiDF: DataFrame = readFromHudi(spark, hudiTablePath)

//    hudiDF.printSchema()
//    hudiDF.show(10)

    hudiDF.persist(StorageLevel.MEMORY_AND_DISK)

    reportProduct(hudiDF)



    //释放资源
    hudiDF.unpersist()
    spark.stop()


  }

}
