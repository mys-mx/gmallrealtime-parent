package com.amos.hudi.didi

import com.amos.hudi.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 滴滴海口出行运营数据分析，使用sparksql操作数据，先读取csv文件，保存至hudi中
 */

object DiDiStorageSpark {
  // 滴滴数据路径
  val dataPath: String = "file:///D:/FLINK/gmallrealtime-parent/hudi-spark/src/main/scala/com/amos/hudi/data/dwv_order_make_haikou_1.txt"

  val hudiTableName: String = "tbl_didi_haikou"
  val hudiTablePath: String = "/hudi-warehouse/tbl_didi_haikou"

  def readCsvFile(spark: SparkSession, dataPath: String): DataFrame = {
    val csvDF = spark.read.
      //   设置分隔符为制表符
      option("sep", "\\t").
      //    文件首列为列名称
      option("header", "true").
      //    依据数值自动推断数据类型
      option("inferSchema", "true").
      csv(dataPath)
    csvDF
  }

  /**
   * 对滴滴出现的海口数据进行一个etl转换操作，指定ts和partitionpath列
   *
   * @param didiDF
   * @return
   */
  def process(didiDF: DataFrame): DataFrame = {
    // 添加字段，就是Hudi表分区字段，三级分区 --> yyyy-MM-dd
    didiDF.withColumn("partitionpath",
      concat_ws("-", col("year"), col("month"), col("day")))
      //删除列
      .drop("year", "day", "month")
      // 添加timestamp列，作为Hudi表记录数据合并字段，使用发车时间
      .withColumn("ts", unix_timestamp(col("departure_time"), "yyyy-MM-dd HH:mm:ss"))
  }

  /**
   * 将数据集dataframe保存至hudi表中，表的类型是cow，属于批量保存数据，写少读多
   *
   * @param etlDF
   * @param hudiTableName
   * @param hudiTablePath
   */
  def saveToHudi(etlDF: DataFrame, hudiTableName: String, hudiTablePath: String): Unit = {
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    etlDF.write.mode(SaveMode.Overwrite)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      //hudi表的属性设置
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "order_id")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), hudiTableName)
      .save(hudiTablePath)


  }

  def main(args: Array[String]): Unit = {
    // 构建sparksession对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    //加载本地csv文件
    val didiDF: DataFrame = readCsvFile(spark, dataPath)


    val etlDF: DataFrame = process(didiDF)

    // 滴滴出行数据ETL处理并保存至Hudi表中
    saveToHudi(etlDF, hudiTableName, hudiTablePath)
    spark.stop()

  }

}
