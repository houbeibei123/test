//package com.rmzx
//import java.util
//
//import org.apache.hudi.QuickstartUtils.DataGenerator
//import org.apache.hudi.QuickstartUtils._
//
//import scala.collection.JavaConversions._
//import org.apache.spark.sql.SaveMode._
//import org.apache.hudi.DataSourceWriteOptions._
//import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
//import org.apache.hudi.config.HoodieWriteConfig._
//import org.apache.spark.sql.SparkSession
//object HudiTest2 {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("HudiTest")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      //      .master("local[3]")
//      .getOrCreate()
//    val tableName = "hudi_trips_cow_3"
//    val basePath = "file:///tmp/hudi_trips_cow_3"
//    val dataGen: DataGenerator = new DataGenerator
////    val list: util.List[HoodieRecord[_ <: HoodieRecordPayload[_ <: AnyRef]]] = dataGen.generateInserts(10)
//
//    val inserts: util.List[String] = convertToStringList(dataGen.generateInserts(10))
//    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
//    df.write.format("hudi").
//      options(getQuickstartWriteConfigs).
////    .option(TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
////    .option(TABLE_TYPE_OPT_KEY, "MERGE_ON_READ")
//      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
//      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
//      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
//      option(TABLE_NAME, tableName).
//      mode(Overwrite).
//      save(basePath)
//
//    spark.stop()
//  }
//}
