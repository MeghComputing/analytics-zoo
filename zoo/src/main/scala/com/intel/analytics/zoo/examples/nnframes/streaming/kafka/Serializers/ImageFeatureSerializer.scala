package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Serializers

import java.util.Map

import org.apache.kafka.common.serialization.Serializer
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.fasterxml.jackson.databind.ObjectMapper

class ImageFeatureSerializer extends org.apache.kafka.common.serialization.Serializer[ImageFeature] {
  
  override def configure(configs : Map[String, _], isKey: Boolean): Unit = {
    
  }
  
  override def serialize(topic: String, data: ImageFeature): Array[Byte] = {
    
    var retVal: Array[Byte] = null
    var objectMapper = new ObjectMapper()
    
    try {      
      retVal = objectMapper.writeValueAsBytes(data)
      println("Successfully serialized ImageFeature object")
    } catch {
      case e: Exception =>  
      println("Error while serializing ImageFeature object")
    }
    
    return retVal
  }
  
  override def close(): Unit = {
    
  }
  
}