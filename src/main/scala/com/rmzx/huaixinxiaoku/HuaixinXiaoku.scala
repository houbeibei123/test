package com.rmzx.huaixinxiaoku

import java.util

import com.alibaba.fastjson.JSON
import com.hankcs.hanlp.collection.AhoCorasick.AhoCorasickDoubleArrayTrie
import com.rmzx.java.{DocumentCompany, HdfsClient, HuaixinInvoke, Ner4perClient, SmallLibraryOptimizeDataBean}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL

object HuaixinXiaoku {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName(HuaixinXiaoku.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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

    val sc = ss.sparkContext

    val client: HdfsClient = new HdfsClient("/fengkong/resources/media_source.xlsx", "/fengkong/resources/information_keywords.xlsx", "/fengkong/resources/health_level_keywords.xlsx")
    client.buildKeywords()
    client.buildKeywords2()
    client.buildKeywords3()
    val media_source_first_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getMedia_source_first)


    val esUrls = "10.26.1.19,10.26.1.29,10.26.1.39"
    val port = "9200"
//    val qurey = "{   \"query\": {     \"range\": {       \"load_time\": {         \"gte\": \"2020-10-27 00:00:00\",         \"lte\": \"2020-10-27 23:59:59\"       }     }   } }"
        val qurey = "{\n    \"query\":{\n      \"match\": {\n        \"doc_id\": \"4085667288723591112\"\n      }\n    }\n}"
//    val qurey = args(0)
    val index = "weibo-data-2020-10"
    val _type = "_doc"
    val indexType = index + "/" + _type
    val username = "data"
    val password = "rmzx#data"
    //从es按照搜索条件搜索数据，并输入账户密码
    import ss.implicits._
    import scala.collection.JavaConverters._
    val ES_READ_CONFIG = Map(
      ConfigurationOptions.ES_NODES -> esUrls,
      ConfigurationOptions.ES_PORT -> port,
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> username,
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS -> password
    )

    //    val value: RDD[(String, Map[String, Object])] =
    val value1 = EsSpark.esJsonRDD(sc, indexType, qurey, ES_READ_CONFIG).map(_._2).map(t => {
      val value = media_source_first_broadcast.value
      val nObject = JSON.parseObject(t)
//      nObject.put("media_primary_classify", "4561541")
//      nObject.put("liulaogen", "hahahahahhah")
      val title = nObject.getString("title")
      val biz = nObject.getString("biz")
      val website_id = nObject.getString("website_id")


      val id = nObject.getString("doc_id")


     val a =  value.parseText(biz).size() +","+ value.parseText(website_id).size()
      nObject.put("bbbbbiz", a)

      val personname =  getPersonFactor("10.20.7.39", 62300, 10 * 1000, "lkadkfa曾国藩", "lkadkfa曾国藩")
      nObject.put("personnamesdfd", personname)

      var relevance = ""
      val closeableHttpClient = HuaixinInvoke.httpClient("http://101.200.60.206:80/api/v1/selection")
      relevance = HuaixinInvoke.get(closeableHttpClient,"http://101.200.60.206:80/api/v1/selection", "-2651766568882013671", "傲视众生_饺子媛", "㋈ //@柒悅Ava:#成毅梦醒长安# 不惧朝堂风云，不怕阴谋陷阱，不忘壮志本心。期待和炎炎子的早日相见！！哥哥好好休息！可以穿着花衬衣度个假了哈！@成毅 //@成毅:#梦醒长安杀青特辑#杀青，再见！梦回长安处，依然似少年。||分享一组新鲜出炉的#梦醒长安特辑# [兔子][兔子][兔子]《梦醒长安》由@成毅 @张予曦 @韩栋 @宣璐 @何晟铭 领衔主演，讲述了“少年天子”李炎踌躇满志，在危机四伏的长安与“第一女保镖”程若鱼携手作战，拨开迷雾的故事。期待早日荧屏相见[兔子][兔子][兔子]http://t.cn/A6buqr05", "http://fgw.linyi.gov.cn/info/1536/7956.htm", "2020-09-18 10:09:01", "新浪微博")
      closeableHttpClient.close()

      nObject.put("website_id", "9424")
      nObject.put("biz","MjM5MzI5NTU3MQ==")
      nObject.put("relevance", relevance)
      nObject.put("title", "高管kdjfkl挪用   违规融资  曹操： 证券 反洗钱 合作 文化")
      nObject.put("content", "relevance")

      println(id)
      (id, nObject)
    })




    //写入es
    EsSpark.saveToEsWithMeta(value1,"weibo-data-test-2020-11/_doc")

    ss.stop()
  }


  def getPersonFactor(host: String, port: Int, timeout: Int, title: String, content: String): String = {
    var person = ""
    var loop = true
    //添加超时最大连接数
    for (i <- 1 to 3 if loop == true) {
      person = Ner4perClient.startClient(host, port, timeout, title, content)
      if (StringUtils.isNotBlank(person)) {
        loop = false
      }
    }
    if (StringUtils.isNotBlank(person) || person.equalsIgnoreCase("null")) {
      person = ""
    }
    person
  }
}
