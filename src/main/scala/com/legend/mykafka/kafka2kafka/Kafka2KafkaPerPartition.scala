package com.legend.mykafka.kafka2kafka

import com.legend.mykafka.utils.MyKafkaUtils._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

import com.legend.mykafka.utils.ParseUtils
import org.apache.spark.streaming.kafka.HasOffsetRanges

/**
  * 将kafka中的数据消费后写入到kafka， 按照partition的方式
  */
object Kafka2KafkaPerPartition {
  def main(args: Array[String]): Unit = {
    val processingInterval = 10
    val brokers = "localhost:9092"
    val topic = "mytest1"
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
        .set("spark.executor.memory","2g")

    val topicsSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "largest")

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))


    val streaming = createMyDirectKafkaStream(ssc, kafkaParams, Set(topic), "testp1") // testp

    val sinkTopic = "mykafka"


    streaming.foreachRDD(rdd=>rdd.foreachPartition(
      partition=>{
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String,String](props)

        partition.map(msg=>ParseUtils.parseMsg(msg._2)).filter(_.length!=1).foreach(msg=>{
          val message=new ProducerRecord[String, String]( sinkTopic ,null,msg)
          producer.send(message)
        })

        saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, "testp1")
      }
    ))

    ssc.start()
    ssc.awaitTermination()
  }
}
