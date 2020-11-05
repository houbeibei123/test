package com.rmzx.huaixinxiaoku

import com.alibaba.fastjson.JSON
import com.hankcs.hanlp.collection.AhoCorasick.AhoCorasickDoubleArrayTrie
import com.rmzx.java.{HdfsClient, Ner4perClient}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark

object HuaixinXiaokuHuisuNonlp {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().appName(HuaixinXiaoku.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .master("local[*]")
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
      .config("es.http.timeout", "15m")
      .config("es.http.retries", "50")
      .config("es.action.heart.beat.lead", "50")
      //      .config("es.query","{   \"query\": {     \"range\": {       \"load_time\": {         \"gte\": \"2020-10-27 00:00:00\",         \"lte\": \"2020-10-27 23:59:59\"       }     }   } }")
      .config("es.net.http.auth.user", "data")
      .config("es.net.http.auth.pass", "rmzx#data")
      .getOrCreate()

    val sc = ss.sparkContext

    val client: HdfsClient = new HdfsClient("/fengkong/resources/media_source.xlsx", "/fengkong/resources/information_keywords.xlsx", "/fengkong/resources/health_level_keywords.xlsx")
    client.buildKeywords()
    client.buildKeywords2()
    client.buildKeywords3()
    //一级媒体站点关键词
    val media_source_first_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getMedia_source_first)
    //二级媒体站点关键词
    val media_source_second_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getMedia_source_second)
    //常规关键词
    val generalKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getGeneralKeywords)
    //机构关键词
    val organizationKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getOrganizationKeywords)
    //事件引导
    val eventGuideKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getEventGuideKeywords)
    //社会责任
    val socialKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getSocialKeywords)
    //群体事件
    val groupEventKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getGroupEventKeywords)
    //高管/董事长/首席  高管关键词
    val positionKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getPositionKeywords)
    //经营发生严重问题模块下的高管危害关键词
    val jobHarmKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getJobHarmKeywords)
    //经营发生严重问题 关键词
    val ecomomicProblemKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getEcomomicProblemKeywords)
    //客户投诉
    val customerComplaintKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getCustomerComplaintKeywords)
    //高管不当言行类下的高管危害关键词
    val wrongWordsKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getWrongWordsKeywords)
    //业务及经营问题
    val managementProblemKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getManagementProblemKeywords)
    //一般性投诉
    val generalQuestionKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getGeneralQuestionKeywords)
    //技术性问题
    val technicalQuestionKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getTechnicalQuestionKeywords)
    //巨额亏损
    val hugeLossKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getHugeLossKeywords)
    //某一传播点受到质疑
    val doubtKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getDoubtKeywords)
    //受监管处罚
    val supervisoryPunishmentKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getSupervisoryPunishmentKeywords)
    //一般员工不当言行
    val employeesKeywords_broadcast: Broadcast[AhoCorasickDoubleArrayTrie[String]] = sc.broadcast(client.getEmployeesKeywords)


    val esUrls = "10.26.1.19,10.26.1.29,10.26.1.39"
    val port = "9200"
//    val qurey = "{\"query\":{ \"bool\": { \"must_not\": { \"term\" : {  \"source_type\" : \"11\"  }  } }}}"
//            val qurey = "{\n    \"query\":{\n      \"match\": {\n        \"doc_id\": \"4085667288723591112\"\n      }\n    }\n}"
        val qurey = args(0)
//    val index = "weibo-data-test-2020-11"
    val index = args(1)
    val _type = "_doc"
    val indexType = index + "/" + _type
    val username = "data"
    val password = "rmzx#data"
    //从es按照搜索条件搜索数据，并输入账户密码
    val ES_READ_CONFIG = Map(
      ConfigurationOptions.ES_NODES -> esUrls,
      ConfigurationOptions.ES_PORT -> port,
      ConfigurationOptions.ES_NET_HTTP_AUTH_USER -> username,
      ConfigurationOptions.ES_NET_HTTP_AUTH_PASS -> password,
      ConfigurationOptions.ES_HTTP_TIMEOUT -> "20m",
      ConfigurationOptions.ES_HTTP_RETRIES -> "50",
      ConfigurationOptions.ES_SCROLL_KEEPALIVE -> "20m",
      ConfigurationOptions.ES_HEART_BEAT_LEAD -> "50",
      ConfigurationOptions.ES_BATCH_SIZE_BYTES -> "300000000",
      ConfigurationOptions.ES_BATCH_SIZE_ENTRIES -> "10000",
      ConfigurationOptions.ES_SCROLL_SIZE -> "1000"
    )
//    es.http.timeout "20m"
//    es.http.retries  "50"
//    es.scroll.keepalive "20m"
//    es.action.heart.beat.lead "50"
//    es.batch.size.bytes "300000000"
//    es.batch.size.entries, "10000"

    //    val value: RDD[(String, Map[String, Object])] =
    val value1 = EsSpark.esJsonRDD(sc, indexType, qurey, ES_READ_CONFIG).map(t => {
      //一级媒体站点关键词
      val media_source_first_data = media_source_first_broadcast.value
      //二级媒体站点关键词
      val media_source_second_data = media_source_second_broadcast.value
      //常规关键词
      val generalKeywords_data = generalKeywords_broadcast.value
      //机构关键词
      val organizationKeywords_data = organizationKeywords_broadcast.value
      //事件引导
      val eventGuideKeywords_data = eventGuideKeywords_broadcast.value
      //社会责任
      val socialKeywords_data = socialKeywords_broadcast.value
      //群体事件
      val groupEventKeywords_data = groupEventKeywords_broadcast.value
      //高管/董事长/首席  高管关键词
      val positionKeywords_data = positionKeywords_broadcast.value
      //经营发生严重问题模块下的高管危害关键词
      val jobHarmKeywords_data = jobHarmKeywords_broadcast.value
      //经营发生严重问题 关键词
      val ecomomicProblemKeywords_data = ecomomicProblemKeywords_broadcast.value
      //客户投诉
      val customerComplaintKeywords_data = customerComplaintKeywords_broadcast.value
      //高管不当言行类下的高管危害关键词
      val wrongWordsKeywords_data = wrongWordsKeywords_broadcast.value
      //业务及经营问题
      val managementProblemKeywords_data = managementProblemKeywords_broadcast.value
      //一般性投诉
      val generalQuestionKeywords_data = generalQuestionKeywords_broadcast.value
      //技术性问题
      val technicalQuestionKeywords_data = technicalQuestionKeywords_broadcast.value
      //巨额亏损
      val hugeLossKeywords_data = hugeLossKeywords_broadcast.value
      //某一传播点受到质疑
      val doubtKeywords_data = doubtKeywords_broadcast.value
      //受监管处罚
      val supervisoryPunishmentKeywords_data = supervisoryPunishmentKeywords_broadcast.value
      //一般员工不当言行
      val employeesKeywords_data = employeesKeywords_broadcast.value


      val dataObject = JSON.parseObject(t._2)

      var title = ""
      var content = ""
      var biz = ""
      var website_id = ""
      if (StringUtils.isNotBlank(dataObject.getString("title"))) {
        title = dataObject.getString("title")
      }
      if (StringUtils.isNotBlank(dataObject.getString("content"))) {
        content = dataObject.getString("content")
      }
      if (StringUtils.isNotBlank(dataObject.getString("biz"))) {
        biz = dataObject.getString("biz")
      }
      if (StringUtils.isNotBlank(dataObject.getString("website_id"))) {
        website_id = dataObject.getString("website_id")
      }

      //garbage_classify
      //garbage_fine_classify  //垃圾细分类 1：标题为垃圾 2：内容为垃圾 3：网站为垃圾 4：内容不足100字符 5：图片数量大于8个  6：标题长度超过40字符 7：标题长度小于6字符 8：微信号为垃圾 9:主体或提及公司数量大于10
      //    							增加两个细分类：//10:舆情数据屏蔽词库
      if(StringUtils.isNotBlank(dataObject.getString("relevance")) && dataObject.getString("relevance").equals("0")){
        dataObject.put("garbage_classify", "1")
          if(StringUtils.isNotBlank(dataObject.getString("garbage_fine_classify"))){
              dataObject.put("garbage_fine_classify",dataObject.getString("garbage_fine_classify") +","+"10")
          }else{
            dataObject.put("garbage_fine_classify","10")
          }
      }



      //knowledge_read  //知识解读类别：0:不符合规则 1：符合规则
      var knowledge_read = "0"
      if (StringUtils.isNotBlank(title) && title.contains("证券") && (title.contains("反洗钱") || title.contains("宣传"))) {
        knowledge_read = "1"
      } else if (StringUtils.isNotBlank(content) && content.contains("证券") && (content.contains("反洗钱") || content.contains("宣传"))) {
        knowledge_read = "1"
      }
      dataObject.put("knowledge_read", knowledge_read)

      //nlp 识别名字
//      var personName = ""
//      if (StringUtils.isNotBlank(title)) {
//        personName = getPersonFactor("10.20.7.39", 62300, 10 * 1000, title, title)
//      }
      // policy_read  //政策解读类别：0:不符合规则 1：符合规则
      var policy_read = "0"
//      if (StringUtils.isNotBlank(title) && StringUtils.isNotBlank(personName) && title.contains("证券")) {
//        if (title.contains(":") || title.contains("：")) {
//          policy_read = "1"
//        }
//      }
      dataObject.put("policy_read", policy_read)

      //      time_boot //时间引导类别：0:不符合规则 1：符合规则
      //      social_responsibility 社会责任类别：//0:不符合规则 1：符合规则
      var time_boot = "0"
      var social_responsibility = "0"
      if (StringUtils.isNotBlank(title) && title.contains("证券")) {
        if (eventGuideKeywords_data.parseText(title).size() > 0){
          time_boot = "1"
        }
        if(socialKeywords_data.parseText(title).size() > 0){
          social_responsibility = "1"
        }
      }
      dataObject.put("time_boot",time_boot)
      dataObject.put("social_responsibility",social_responsibility)

//      media_level  //媒体级别打标   0:其他 1:一级媒体 2：二级媒体
      dataObject.put("media_level","0")
      if(dataObject.getLongValue("website_id") != 0){
       val media_first =  media_source_first_data.parseText(dataObject.getLongValue("website_id") + "").size()
        var media_second = 0
        if(StringUtils.isNotBlank(dataObject.getString("biz")) ){
            media_second = media_source_second_data.parseText(dataObject.getString("biz")).size()
        }
        if(media_first > 0){
          dataObject.put("media_level","1")
        }else if(media_second > 0){
          dataObject.put("media_level","2")
        }
      }else{
        var media_second = 0
        if(StringUtils.isNotBlank(dataObject.getString("biz")) ){
          media_second = media_source_second_data.parseText(dataObject.getString("biz")).size()
        }
        if(media_second > 0){
          dataObject.put("media_level","2")
        }
      }

      dataObject.put("industry_keywords","0")
      dataObject.put("industry_body","0")
      dataObject.put("health_level","0")
      dataObject.put("health_level_subsection","0")
      if(!dataObject.getString("media_level").equals("0")){
//        industry_keywords //行业信息-常规关键字打标   0：不符合关键词提取规则 1：符合关键词提取规则
          if(StringUtils.isNotBlank(title) && generalKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && generalKeywords_data.parseText(content).size() > 0){
              dataObject.put("industry_keywords","1")
          }
        // industry_body //行业信息-机构打标	 0：不符合关键词提取规则 1：符合关键词提取规则
        if(StringUtils.isNotBlank(title) && organizationKeywords_data.parseText(title).size() > 0
          || StringUtils.isNotBlank(content) && organizationKeywords_data.parseText(content).size() > 0){
          dataObject.put("industry_body","1")
        }

        // health_level //舆情健康度打标  0:其他 1：关注 2：有害 3：危害   一级是关注 二级是有害 三级是危害
        // health_level_subsection //舆情健康度细分打标 0：其他 1：群体事件 2：经营发生严重问题 3：客户投诉 4：高管不当言行类 5：业务及经营问题 6：一般性投诉
        // 7：技术性问题 8：巨额亏损 9：某一传播点受到质疑 10：受监管处罚 11：一般员工不当言行

        if(StringUtils.isNotBlank(title) && groupEventKeywords_data.parseText(title).size() > 0
          || StringUtils.isNotBlank(content) && groupEventKeywords_data.parseText(content).size() > 0){
          dataObject.put("health_level","3")
          dataObject.put("health_level_subsection","1")
        }else if(
          (StringUtils.isNotBlank(title) && positionKeywords_data.parseText(title).size() > 0 && jobHarmKeywords_data.parseText(title).size() > 0)
          ||
            (StringUtils.isNotBlank(content) && positionKeywords_data.parseText(content).size() > 0 && jobHarmKeywords_data.parseText(content).size() > 0)
          ||
            (StringUtils.isNotBlank(title) && ecomomicProblemKeywords_data.parseText(title).size()>0 ||
              StringUtils.isNotBlank(content) && ecomomicProblemKeywords_data.parseText(content).size()>0)
        ){
          dataObject.put("health_level","3")
          dataObject.put("health_level_subsection","2")
        }else if(StringUtils.isNotBlank(title) && customerComplaintKeywords_data.parseText(title).size() > 0
          || StringUtils.isNotBlank(content) && customerComplaintKeywords_data.parseText(content).size() > 0){
          dataObject.put("health_level","2")
          dataObject.put("health_level_subsection","3")
        }else if(
          (StringUtils.isNotBlank(title) && positionKeywords_data.parseText(title).size() > 0 && wrongWordsKeywords_data.parseText(title).size() > 0)
          ||
        (StringUtils.isNotBlank(content) && positionKeywords_data.parseText(content).size() > 0 && wrongWordsKeywords_data.parseText(content).size() > 0)
        ){
          dataObject.put("health_level","2")
          dataObject.put("health_level_subsection","4")
        }else if(
          StringUtils.isNotBlank(title) && managementProblemKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && managementProblemKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","2")
          dataObject.put("health_level_subsection","5")
        }else if(
          StringUtils.isNotBlank(title) && generalQuestionKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && generalQuestionKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","1")
          dataObject.put("health_level_subsection","6")
        }else if(
          StringUtils.isNotBlank(title) && technicalQuestionKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && technicalQuestionKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","1")
          dataObject.put("health_level_subsection","7")
        }else if(
          StringUtils.isNotBlank(title) && hugeLossKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && hugeLossKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","1")
          dataObject.put("health_level_subsection","8")
        }else if(
          StringUtils.isNotBlank(title) && doubtKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && doubtKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","1")
          dataObject.put("health_level_subsection","9")
        }else if(
          StringUtils.isNotBlank(title) && supervisoryPunishmentKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && supervisoryPunishmentKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","1")
          dataObject.put("health_level_subsection","10")
        }else if(
          StringUtils.isNotBlank(title) && employeesKeywords_data.parseText(title).size() > 0
            || StringUtils.isNotBlank(content) && employeesKeywords_data.parseText(content).size() > 0
        ){
          dataObject.put("health_level","1")
          dataObject.put("health_level_subsection","11")
        }
      }

      (t._1, dataObject)
    })

//      .map(t => t._1 +","+ t._2.getString("garbage_classify")+"#"+ t._2.getString("garbage_fine_classify")+"#"+ t._2.getString("knowledge_read")
//        +"#"+ t._2.getString("time_boot")+"#"+ t._2.getString("social_responsibility")+"#"+ t._2.getString("media_level")
//        +"#"+ t._2.getString("industry_keywords")+"#"+ t._2.getString("industry_body")+"#"+ t._2.getString("health_level")
//        +"#"+ t._2.getString("health_level_subsection"))
//      .foreach(println)


    //写入es
        EsSpark.saveToEsWithMeta(value1,indexType)

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
