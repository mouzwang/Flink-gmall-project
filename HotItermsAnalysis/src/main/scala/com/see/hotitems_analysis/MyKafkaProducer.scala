package com.see.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by mouzwang on 2019-10-24 19:17
  */
object MyKafkaProducer {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop101:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //创建生产者
    val producer = new KafkaProducer[String,String](properties)
    //从文件中读取数据
    val bufferSource = io.Source.fromFile("/Users/mouzwang/idea-workspace/flink-project/HotItermsAnalysis/src/main/resources/UserBehavior.csv")
    for (line <- bufferSource.getLines()) {
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}