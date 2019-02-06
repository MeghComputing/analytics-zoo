package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Consumers

import java.util.Properties
import java.util.Collections

import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Deserializers._

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.common.serialization.StringDeserializer


object ImageConsumerFactory {
  
  val KAFKA_BROKERS = "222.10.0.50:9092"
  val GROUP_ID = "consumerGroup1"
  val TOPIC: String = "imagestream1"
  val MAX_POLL_RECORDS: Integer = 1
  val OFFSET_RESET_EARLIER = "earliest"
  
  def createConsumer : KafkaConsumer[String, ImageFeature] = {
    var prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS)
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ImageFeatureDeserializer])
    prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS)
    prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER)
    
    var consumer = new KafkaConsumer[String, ImageFeature](prop)
    consumer.subscribe(Collections.singletonList(TOPIC))
    
    consumer
  }
  
}