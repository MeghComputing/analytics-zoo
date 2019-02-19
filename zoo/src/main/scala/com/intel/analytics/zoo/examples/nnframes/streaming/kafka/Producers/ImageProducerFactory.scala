package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.util.Properties
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Serializers._

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.common.serialization.StringSerializer


object ImageProducerFactory {
  
  def createProducer(brokers: String, clientId: String) : KafkaProducer[String, ImageFeature] = {
    var prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[CustomObjectSerializer])
    
    return new KafkaProducer[String, ImageFeature](prop);
  }
  
}