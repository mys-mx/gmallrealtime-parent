package com.amos.hudi

import com.amos.crud.DeleteData.deleteData
import com.amos.crud.IncrementalQueryData.incrementalQueryData
import com.amos.crud.InsertData.insertData
import com.amos.crud.QueryData.{queryData, queryDataByTime}
import com.amos.crud.UpdateData.updateData
import org.apache.spark.sql.{SaveMode, SparkSession}

object HudiSparkDemo {
  //定义变量:表名称、保存路径
  val tableName: String = "tbl_trips_cow"
  val tablePath: String = "/hudi-warehouse/tbl_trips_cow"


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }

    //构建数据生成器，模拟产生业务数据
    import org.apache.hudi.QuickstartUtils._

    val dataGene: DataGenerator = new DataGenerator()
    // 任务一：模拟数据，插入Hudi表，采用cow模式
    //    insertData(spark, tableName, tablePath, dataGene)

    //任务二：快照方式查询（snapshot Query）数据，采用DSL方式
    //    queryData(spark, tablePath)
    //    queryDataByTime(spark, tablePath)


    /** 任务三：更新数据，第一步模拟产生数据，
     * 第二步模拟产生数据对第一步数据字段更新，
     * 第三步将数据更新到Hudi
     */
    //    insertData(spark, tableName, tablePath, dataGene)
    //    updateData(spark, tableName, tablePath, dataGene)
    //    queryData(spark, tablePath)

    //    incrementalQueryData(spark, tablePath)

    //删除数据
    deleteData(spark, tableName, tablePath)

    //应用结束，关闭资源
    spark.stop()
  }

}
