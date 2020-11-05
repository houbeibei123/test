package com.rmzx



import com.alibaba.fastjson.JSON
import com.rmzx.utils.DateFormatHour
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.QuickstartUtils.DataGenerator
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.HoodieWriteClient
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object HudiWrite {
  def main(args: Array[String]): Unit = {
    val format = new DateFormatHour
    val spark = SparkSession.builder().appName("HudiWrite")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .master("local[3]")
      .getOrCreate()
    val tableName = args(1)
    val basePath = args(2)
//    val cfg: HoodieWriteConfig = HoodieWriteConfig.newBuilder()
//      .withPath(basePath)
//      .forTable(tableName)
//      .withCompactionConfig(HoodieCompactionConfig.newBuilder().withAutoClean(false).build())
//      .build()
//
//    val value = new HoodieWriteClient(spark.sparkContext,cfg)

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
      option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE").
//          option(TABLE_TYPE_OPT_KEY, "MERGE_ON_READ").
      option(PRECOMBINE_FIELD_OPT_KEY, "pubTime").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(PARTITIONPATH_FIELD_OPT_KEY, "createDate").
      option(TABLE_NAME, tableName).
//      option(HIVE_SYNC_ENABLED_OPT_KEY, "true").
//      option(HIVE_TABLE_OPT_KEY, tableName).
//      option(HIVE_PARTITION_FIELDS_OPT_KEY, "createDate").
//      option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).
      mode(Append).
      save(basePath)

      df.write.format("org.apache.hudi").
      option(RECORDKEY_FIELD_OPT_KEY, "c_class").
      option(PARTITIONPATH_FIELD_OPT_KEY, "state").
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, tableName).
      option(HIVE_SYNC_ENABLED_OPT_KEY, "true").
      option(HIVE_TABLE_OPT_KEY, "hivehudi").
      option(HIVE_PARTITION_FIELDS_OPT_KEY, "state").
      // 设置要同步的hive库名
      option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
      // 设置当分区变更时，当前数据的分区目录是否变更
      option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true").
      // hudi表名称设置
      option(HoodieWriteConfig.TABLE_NAME, "hivehudi").
      option(HIVE_USER_OPT_KEY,"people").
      option(HIVE_PASS_OPT_KEY,"People-2020").
      option(HIVE_URL_OPT_KEY,"jdbc:hive2://10.19.2.54:10001").
      option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).
      mode(Overwrite).
      save(basePath)





    spark.stop()
  }
}
