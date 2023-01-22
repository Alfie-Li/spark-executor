package com.legend.mykafka.kafka2kafka

import java.util.Properties

import com.legend.mykafka.producer._
import com.legend.mykafka.utils.MyKafkaUtils._
import com.legend.mykafka.utils.ParseUtils
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 23-1-22.
  */
object Kafka2KafkaPerBatch {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("test")
      .setMaster("local[*]")
//      .set("spark.local.dir", "/spark-tmp")
//      .set("spark.driver.memory", "2g")
//      .set("spark.executor.memory", "4g")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    val processingInterval = 2
    val brokers = "localhost:9092"
    val topic = "mytest1"
    val topicSet = topic.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")

    val ssc = new StreamingContext(sc, Seconds(processingInterval))

    val streaming = createMyDirectKafkaStream(ssc, kafkaParams, Set(topic), "testp")

    val sinkTopic = "myKafka"


    /**
      * Kafka的Producer不能序列化
      * Caused by: java.io.NotSerializableException: org.apache.kafka.clients.producer.KafkaProducer
S       erialization stack:
	      -     object not serializable (class: org.apache.kafka.clients.producer.KafkaProducer, value: org.apache.kafka.clients.producer.KafkaProducer@1209f269)
      */
    /*streaming.foreachRDD(rdd =>{
      if(!rdd.isEmpty()){
        import java.util
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        rdd.map(x => x._2).map(msg => ParseUtils.parseMsg(msg)).foreach(msg =>{
          val message = new ProducerRecord[String, String](sinkTopic, null, msg)
          producer.send(message)
        })
      }
    })*/

    // Caused by: java.lang.OutOfMemoryError: unable to create new native thread
    // 数据可以写入到kafka， 但是性能差， 每条记录都需要创建producer
    /*streaming.foreachRDD(rdd =>{
      if(!rdd.isEmpty()){
        import java.util


        rdd.map(x => x._2).map(msg => ParseUtils.parseMsg(msg)).filter(_.length != 1).foreach(msg =>{
          val props = new util.HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")

          val producer = new KafkaProducer[String, String](props)

          val message = new ProducerRecord[String, String](sinkTopic, null, msg)
          producer.send(message)
        })
      }
    })*/

    // 将KafkaProducer对象广播到所有的executor节点， 这样就可以在每个executor节点将数据插入到kafka
    val kafkaProducer: Broadcast[MyKafkaProducer[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "localhost:9092,localhost:19092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }

      ssc.sparkContext.broadcast(MyKafkaProducer[String, String](kafkaProducerConfig))
    }

    streaming.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        rdd.map(x=>x._2).map(msg=>ParseUtils.parseMsg(msg)).filter(_.length!=1).foreach(msg=>{
          kafkaProducer.value.send(sinkTopic, msg)
        }
        )
        saveOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, "testp")
      }})

    ssc.start()
    ssc.awaitTermination()
  }

}
