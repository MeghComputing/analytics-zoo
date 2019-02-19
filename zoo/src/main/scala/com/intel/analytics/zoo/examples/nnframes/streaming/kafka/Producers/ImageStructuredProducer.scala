package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.{ByteArrayOutputStream, File}

import java.util.UUID
import java.util.Base64
import java.util.Properties

import util.control.Breaks._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.io.FileUtils;

import scala.actors.threadpool.ExecutionException

import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.opencv.core.{CvType, Mat}


import org.apache.kafka.common.serialization.StringSerializer
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Serializers._

import com.google.gson.JsonObject
import com.google.gson.Gson

object ImageStructuredProducer {
  
  val TOPIC: String = "imagestream1"
  
  def main(args: Array[String]): Unit = {
    
    val KAFKA_BROKERS = "222.10.0.51:9092"
    val CLIENT_ID = "client1"
    
    var prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS)
    prop.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  
    
    val producer = new KafkaProducer[String, String](prop)
    
    /*if(!Files.notExists(Paths.get(args(0))))
    {
      throw new IllegalArgumentException("check command line arguments")
    }*/
    
    val dir = new File(args(0)).listFiles
    var count = 0
    
    if(dir != null) {
      for(file <- dir) {
        try {
          
          val imageName = file.getName()
          
          println("Read from image path:");
          println(file.getAbsolutePath());
                
          println("Image size:");
          println(Math.toIntExact(file.length()));
          
          val data = FileUtils.readFileToByteArray(file)                 
          
          if(data == null)
          {
            println("Error reading from file")
            break
          }
          
          var obj = new JsonObject()
          var gson = new Gson()
          obj.addProperty("origin", imageName)
          obj.addProperty("height", 0)
          obj.addProperty("width", 0)
          obj.addProperty("nChannels", 0)
          obj.addProperty("mode", 0)
          obj.addProperty("data", Base64.getEncoder.encodeToString(data))  
          
          val record: ProducerRecord[String, String] = new ProducerRecord(TOPIC, UUID.randomUUID.toString, gson.toJson(obj))
          producer.send(record)
          
          count = count + 1
          println("Image successfully sent " + count)
          
          Thread.sleep(args(1).toInt);          
        }
        catch {
          case e: ExecutionException =>
            println("Error in sending record")
            e.printStackTrace()
          case e: InterruptedException =>
            println("Error in sending record")
            e.printStackTrace()
        }        
      }
    }
    
    producer.close()
      
    
  }
  
}