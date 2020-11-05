package com.rmzx.huaixinxiaoku

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

object HuaixinXiaokuSpark {
  def main(args: Array[String]): Unit = {
    val ssc = SparkSession.builder().appName("app")
            .master("local[*]")
      .config("es.index.auto.create", "true")
      .config("pushdown", "true")
      .config("es.nodes", "10.26.1.19,10.26.1.29,10.26.1.39")
      .config("es.port", "9200")
      .config("es.nodes.wan.only", "true")
      .config("es.batch.write.retry.wait", "500")
      .config("es.batch.write.retry.count", "50")
      .config("es.batch.size.bytes", "300000000")
      .config("es.batch.size.entries", "10000")
      .config("es.batch.write.refresh", "false")
      .config("es.batch.write.retry.count", "60")
      .config("es.http.timeout", "10m")
      .config("es.http.retries", "50")
      .config("es.action.heart.beat.lead", "50")
//      .config("es.query","{   \"query\": {     \"range\": {       \"load_time\": {         \"gte\": \"2020-10-27 00:00:00\",         \"lte\": \"2020-10-27 23:59:59\"       }     }   } }")
      .config("es.net.http.auth.user","data")
      .config("es.net.http.auth.pass","rmzx#data")
      .getOrCreate()


    val index = "weibo-data-2020-10"
    val _type = "_doc"
    val indexType = index + "/" + _type

    val sc = ssc.sparkContext

    sc.esRDD(indexType).map(_._2).foreach(println)


    ssc.stop()

  }
}
