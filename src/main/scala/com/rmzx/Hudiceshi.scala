package com.rmzx

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.HoodieWriteClient
import org.apache.hudi.common.model.HoodieRecordPayload
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.hive.MultiPartKeysValueExtractor


object Hudiceshi {
  def main(args: Array[String]): Unit = {
    val ssc = SparkSession.builder().appName("HudiWrite")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .master("local[3]")
      .getOrCreate()

    val tableName = "hudi_ceshi"
    val basePath = "/hudidata/hudi_ceshi"

    val sc = ssc.sparkContext

//    val cfg: HoodieWriteConfig = HoodieWriteConfig.newBuilder()
//      .withPath(basePath)
//      .forTable(tableName)
//      .withCompactionConfig(HoodieCompactionConfig.newBuilder().withAutoClean(false).build())
//      .build()
//
//    val value = new HoodieWriteClient(sc,cfg).upsert()


    val lineRDD = sc.textFile(args(0)).map(_.split("\\|")).filter(_.length > 6)

    import ssc.implicits._
    val RecordRDD = lineRDD.map(x => Record(x(0).toInt, x(1), x(2), x(3), x(4).toFloat, x(5), x(6).toInt)).toDF()

    DataSourceReadOptions

    RecordRDD.write.format("org.apache.hudi").
      option(RECORDKEY_FIELD_OPT_KEY, "c_class").
      option(PARTITIONPATH_FIELD_OPT_KEY, "state").
      option(PRECOMBINE_FIELD_OPT_KEY, "id").
      option(TABLE_NAME, tableName).

      mode(Append).
      save(basePath)





    sc.stop()
  }

  case class Record(id: Int, name: String, c_class: String, state: String, latitude: Float, longitude: String, elevation: Int)
}
