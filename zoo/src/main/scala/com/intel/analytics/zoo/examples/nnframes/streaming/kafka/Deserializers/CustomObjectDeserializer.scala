package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Deserializers

import java.util.Map
import java.util.Base64

import org.apache.kafka.common.serialization.Deserializer

import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.CustomObject._
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

class CustomObjectDeserializer extends org.apache.kafka.common.serialization.Deserializer[Object] {
  
  override def configure(configs : Map[String, _], isKey: Boolean): Unit = {
    
  }
  
  /*override def deserialize(topic: String, data: Array[Byte]): Object = {  
        
    val bis = new ByteArrayInputStream(data)
    var in: ObjectInput = null
    var o: Object = null
    try {
        in = new ObjectInputStream(bis);
        o = in.readObject();
    }catch {
      case e: Exception =>
        println("Error while deserialization")
        print(e.getMessage)
        e.printStackTrace()
    }
    finally {
        try {
            if (in != null) {
                in.close();
            }
        } catch {
            case e: Exception =>
              println("Error while deserialization")
              print(e.getMessage)
              e.printStackTrace()
        }
    }
    o
  }*/

  override def deserialize(topic: String, data: Array[Byte]): Object = {  
        
    /*val mapper = new ObjectMapper()
    var intermedObj: JSonImage = null
    var o: ImageFeature = null
    try {
        //val tempData: Array[Byte] = Base64.getMimeDecoder().decode(data)
        //o = mapper.readValue(tempData, classOf[ImageFeature])
        intermedObj = mapper.readValue(data, classOf[JSonImage])
        o = ImageFeature(Base64.getMimeDecoder().decode(intermedObj.base64Data), uri=intermedObj.name)

        //o = mapper.convertValue(data, classOf[ImageFeature])
    }catch {
      case e: Exception =>
        println("Error while deserialization")
        print(e.getMessage)
        e.printStackTrace()
    }*/
    
    val bis = new ByteArrayInputStream(data);
    var in: ObjectInputStream = null;
    var o: Object = null;
    try {
        in = new ObjectInputStream(bis);
        o = in.readObject();
    } catch  {
      case e: Exception =>
        println("Error while deserialization")
        print(e.getMessage)
        e.printStackTrace()
    }
    finally {
        try {
            if (in != null) {
                in.close();
            }
        } catch  {
          case e: IOException =>
            println("Error while deserialization")
            print(e.getMessage)
            e.printStackTrace()
        }
    }

    o
  }
  
  override def close(): Unit = {
    
  }
  
}