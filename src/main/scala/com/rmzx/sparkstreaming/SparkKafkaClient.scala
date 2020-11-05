package sparkkafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}



object SparkKafkaClient {
  def main(args: Array[String]): Unit = {
    val group = "fffff"
    val topic = "test-lll"
    //创建SparkConf，如果将任务提交到集群中，那么要去掉.setMaster("local[4]")
    val conf = new SparkConf().setAppName("DirectStream").setMaster("local[*]")
    //创建一个StreamingContext，其里面包含了一个SparkContext
    val ssc = new StreamingContext(conf, Seconds(5));

    //配置kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.19.2.54:9092,10.19.2.55:9092,10.19.2.56:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest", // lastest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)
    //在Kafka中记录读取偏移量
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      //位置策略
      PreferConsistent,
      //订阅的策略
      Subscribe[String, String](topics, kafkaParams)
    )


    //迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
    stream.foreachRDD { rdd =>
      //获取该RDD对于的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //拿出对于的数据，foreach是一个aciton
      //todo
      //val results = yourCalculation(rdd)
      rdd.foreach { line =>
        println(line.key() + " " + line.value())
      }
      //更新偏移量
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
