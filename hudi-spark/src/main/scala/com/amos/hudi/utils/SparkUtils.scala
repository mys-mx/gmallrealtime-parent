package com.amos.hudi.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def createSparkSession(clazz: Class[_], master: String = "local[2]", partitions: Int = 4): SparkSession = {
    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master(master)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", partitions)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession(this.getClass)
    println(spark)
    Thread.sleep(1000)
    spark.stop()
  }
}
