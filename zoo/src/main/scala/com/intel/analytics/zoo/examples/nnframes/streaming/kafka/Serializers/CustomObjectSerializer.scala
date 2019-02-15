package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Serializers

import java.util.Map
import java.util.Base64

import org.apache.kafka.common.serialization.Serializer
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.fasterxml.jackson.databind.ObjectMapper

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

class CustomObjectSerializer extends org.apache.kafka.common.serialization.Serializer[Object] {
  
  override def configure(configs : Map[String, _], isKey: Boolean): Unit = {
    
  }  
  
  override def serialize(topic: String, data: Object): Array[Byte] = {
    
    var retVal: Array[Byte] = null
    var objectMapper = new ObjectMapper()
    
    try {
      val baos = new ByteArrayOutputStream();
      val oos = new ObjectOutputStream(baos);
      oos.writeObject(data);
      oos.close();
      retVal = baos.toByteArray();      
    } catch {
      case e: Exception =>  
      println("Error while serializing ImageFeature object")
    }

    /*try {      
      //etVal = objectMapper.writeValueAsBytes(data)
      //retVal = objectMapper.convertValue(data, classOf[Array[Byte]])

      retVal = objectMapper.writeValueAsString(data).getBytes()
      println("Successfully serialized ImageFeature object")
    } catch {
      case e: Exception =>  
      println("Error while serializing ImageFeature object")
    }*/
    //retVal = Base64.getMimeEncoder().encode(retVal)
    
    return retVal
  }
  
  override def close(): Unit = {
    
  }
  
}