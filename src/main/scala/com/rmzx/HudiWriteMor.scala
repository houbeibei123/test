package com.rmzx



import com.alibaba.fastjson.JSON
import com.rmzx.utils.DateFormatHour
import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HudiWriteMor {
  def main(args: Array[String]): Unit = {
    val format = new DateFormatHour
    val spark = SparkSession.builder().appName("HudiWrite")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .master("local[3]")
      .getOrCreate()
    val tableName = args(1)
    val basePath = args(2)


    val jsonUpdate: RDD[String] = spark.sparkContext.textFile(args(0)).map(t => {
      val value = JSON.parseObject(t)
      val lg = value.getString("createTime")
      val str1 = format.getDay(lg)
      value.put("createDate", str1)

      val str2 = value.toString
      str2
    })



    val df = spark.read.json(jsonUpdate)
    //        df.select("createDate","id").show()
    df.write.format("org.apache.hudi").
      options(getQuickstartWriteConfigs).
      //      option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE").
      option(TABLE_TYPE_OPT_KEY, "MERGE_ON_READ").
      option(PRECOMBINE_FIELD_OPT_KEY, "pubTime").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(PARTITIONPATH_FIELD_OPT_KEY, "createDate").
      option(TABLE_NAME, tableName).
      mode(SaveMode.Append).
      save(basePath)

    spark.stop()
  }
}
