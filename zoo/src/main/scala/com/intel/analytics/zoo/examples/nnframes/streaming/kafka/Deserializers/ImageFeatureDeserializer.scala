package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Deserializers

import java.util.Map

import org.apache.kafka.common.serialization.Deserializer
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Map;

class PayloadDeserializer extends org.apache.kafka.common.serialization.Deserializer[Object] {
  
  override def configure(configs : Map[String, _], isKey: Boolean): Unit = {
    
  }
  
  override def deserialize(topic: String, data: Array[Byte]): Object = {  
        
    val bis = new ByteArrayInputStream(data)
    var in: ObjectInput = null
    var o: Object = null
    try {
        in = new ObjectInputStream(bis);
        o = in.readObject();
    }catch {
      case e: Exception =>
        println("Error while deserialization")
    }
    finally {
        try {
            if (in != null) {
                in.close();
            }
        } catch {
            case e: Exception =>
              println("Error while deserialization")
        }
    }
    o
  }
  
  override def close(): Unit = {
    
  }
  
}