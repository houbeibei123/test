package com.rmzx

import com.rmzx.utils.DateFormatHour
import org.apache.spark.sql.{DataFrame, SparkSession}

object HudiRead {
  def main(args: Array[String]): Unit = {
    val format = new DateFormatHour
    val spark = SparkSession.builder().appName("HudiWrite")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .master("local[3]")
      .getOrCreate()

    val basePath = args(0)
    val sql = args(1)

    val start = System.currentTimeMillis()

    val toViewDF = spark.read.format("org.apache.hudi").load(basePath + "/*/*")
    toViewDF.registerTempTable("artilceday")
    val rs: DataFrame = spark.sql(sql)

    val queryEnd = System.currentTimeMillis()

    val l = queryEnd - start
//    rs.write.mode("overwrite").save("/hudidata/hudiread/"+ start+"-"+l)
    rs.write.mode("overwrite").json("/hudidata/hudiread/"+ start+"-"+l)

    val jobEnd = System.currentTimeMillis()
    val t = jobEnd - queryEnd
    spark.sparkContext.parallelize(List(1,2,4,5)).saveAsTextFile("/hudidata/hudiread/"+ start+"-"+t)



    spark.stop()
  }
}
