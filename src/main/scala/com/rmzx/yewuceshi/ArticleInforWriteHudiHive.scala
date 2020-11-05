package com.rmzx.yewuceshi

import com.alibaba.fastjson.JSON
import com.rmzx.utils.{DateFormatHour, IsLong}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, HIVE_PARTITION_FIELDS_OPT_KEY, HIVE_PASS_OPT_KEY, HIVE_SYNC_ENABLED_OPT_KEY, HIVE_TABLE_OPT_KEY, HIVE_URL_OPT_KEY, HIVE_USER_OPT_KEY, PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY, TABLE_TYPE_OPT_KEY}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ArticleInforWriteHudiHive {
  def main(args: Array[String]): Unit = {
    val format = new DateFormatHour
    val spark = SparkSession.builder().appName("ArticleInforWriteHudiHive")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .master("local[3]")
      .getOrCreate()

    val tableName = args(1)
    val basePath = args(2)

    import spark.implicits._
    val value = spark.sparkContext.textFile(args(0))

    val articleDataframe: DataFrame = value.filter(t => {
      val splits = t.split("\t", -1)
      splits.length > 100 && !splits(39).equals("") && !splits(39).equals("null") && format.isValidDate(splits(39))
    }).map(t => {
      t + "\t\t\t\t\t\t\t\t\t\t"
    }).map(t => {
      val splits = t.split("\t", -1)


      val partionpath = format.getMonth4(splits(39))
      val precombine = format.getTime(splits(39))

      ArticleInfoHive(splits.head, splits(1), splits(2), splits(3), splits(4), splits(5), splits(6), splits(7), splits(8), splits(9),
        splits(10), splits(11), splits(12), splits(13), splits(14), splits(15), splits(16), splits(17), splits(18), splits(19),
        splits(20), splits(21), splits(22), splits(23), splits(24), splits(25), splits(26), splits(27), splits(28), splits(29),
        splits(30), splits(31), splits(32), splits(33), splits(34), splits(35), splits(36), splits(37), splits(38), splits(39),
        splits(40), splits(41), splits(42), splits(43), splits(44), splits(45), splits(46), splits(47), splits(48), splits(49),
        splits(50), splits(51), splits(52), splits(53), splits(54), splits(55), splits(56), splits(57), splits(58), splits(59),
        splits(60), splits(61), splits(62), splits(63), splits(64), splits(65), splits(66), splits(67), splits(68), splits(69),
        splits(70), splits(71), splits(72), splits(73), splits(74), splits(75), splits(76), splits(77), splits(78), splits(79),
        splits(80), splits(81), splits(82), splits(83), splits(84), splits(85), splits(86), splits(87), splits(88), splits(89),
        splits(90), splits(91), splits(92), splits(93), splits(94), splits(95), splits(96), splits(97), splits(98), splits(99),
        splits(100), splits(101), splits(102), splits(103), splits(104), splits(105), splits(106), partionpath, precombine)

    }).toDF()


    articleDataframe.repartition(30).write.format("org.apache.hudi").
      option("hoodie.insert.shuffle.parallelism", "30").
      option("hoodie.upsert.shuffle.parallelism", "30").
      option(RECORDKEY_FIELD_OPT_KEY, "id").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partionpath").
      option(PRECOMBINE_FIELD_OPT_KEY, "precombine").
      option(TABLE_NAME, tableName).
      option(HIVE_SYNC_ENABLED_OPT_KEY, "true").
      option(HIVE_TABLE_OPT_KEY, tableName).
      option(HIVE_PARTITION_FIELDS_OPT_KEY, "partionpath").
      // 设置要同步的hive库名
      option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default").
      // 设置当分区变更时，当前数据的分区目录是否变更
      option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true").
      option(HIVE_USER_OPT_KEY, "people").
      option(HIVE_PASS_OPT_KEY, "People-2020").
      option(HIVE_URL_OPT_KEY, "jdbc:hive2://10.19.2.54:10001").
      option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName).
      mode(Append).
      save(basePath)

    spark.stop()
  }

  case class ArticleInfoHive(id: String, title: String, author: String, content: String, crawltime: String, urlmd5: String, url: String, areaattr: String, plusattr: String, keywordattr: String,
                             moduleattr: String, peopleattr: String, companyattr: String, departattr: String, productattr: String, platformattr: String, companyareaattr: String, ruleattr: String, plusruleattr: String, titlefingerprint: String,
                             emotionalindex: String, delflag: String, createeditor: String, lastupeditor: String, lastuptime: String, trade: String, companyproductattr: String, hoteventattr: String, language: String, issaveoriginal: String,
                             serviceid: String, emotionalvalue: String, originallenth: String, originalcontent: String, articletype: String, artkeyword: String, artabstract: String, pubsource: String, copyrightsource: String, pubtime: String,
                             websiteid: String, childsiteid: String, fingerprint: String, isoverseasart: String, channelid: String, hit: String, reply: String, heat: String, pagecodeid: String, pagenameid: String,
                             pagesort: String, ismainpagenews: String, copyrightsourceid: String, reservation1: String, reservation2: String, reservation3: String, reservation4: String, ext1: String, ext2: String, ext3: String,
                             ext4: String, ext5: String, ext6: String, ext7: String, ext8: String, createtime: String, wordcount: String, isoverwall: String, sort_id: String, industryids: String,
                             icpids: String, areaids: String, areaclass: String, site_language: String, islistedcompany: String, iscause: String, isemphasis: String, isgeneral: String, institutionids: String, newspapernet: String,
                             isbroadcast: String, site_weight: String, doc_weight: String, pagecode: String, pagename: String, headurl: String, imageurl: String, weixinbiz: String, simhash: String, appsourceurl: String,
                             primaryflag: String, excludeattr: String, excludeattr1: String, excludeattr2: String, extendmultiattr1: String, extendmultiattr2: String, processtime: String, meidiaweight: String, topicareaids: String, pantopicareaids: String,
                             whiteclass: String, basesortzy: String, sortzy: String, fmoduleid: String, polysemyattr: String, year: String, month: String, partionpath: String, precombine: Long
                            )

}
