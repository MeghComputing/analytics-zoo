package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.File



import java.util.UUID
import java.util.Base64
import java.util.Properties

import util.control.Breaks._


import org.apache.commons.io.FileUtils

import scopt.OptionParser

import scala.actors.threadpool.ExecutionException

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig

import org.apache.kafka.common.serialization.StringSerializer

import com.google.gson.JsonObject
import com.google.gson.Gson

object ImageStructuredProducer {
  
  val TOPIC: String = "imagestream1"
  
  def main(args: Array[String]): Unit = {
    
    case class KafkaProducerParam(brokers: String = "",
                                  clientId: String = "",
                                  imageFolder: String = "",
                                  topic: String = "",
                                  txDelay: Int= 0,
                                  numPartitions: Int=1)
  
    val parser = new OptionParser[KafkaProducerParam]("Image Kafka Producer") {
      head("Image Kafka Producer demo")
     
      opt[String]("brokers")
        .text("broker list to connect to")
        .action((x, c) => c.copy(brokers = x))
        .required() 
        
      opt[String]("clientId")
        .text("Client Id")
        .action((x, c) => c.copy(clientId = x))
        .required()
        
      opt[String]("imageFolder")
        .text("Image folder location")
        .action((x, c) => c.copy(imageFolder = x))
        .required()
        
      opt[String]("topic")
        .text("Kafka Topic")
        .action((x, c) => c.copy(topic = x))
        .required()
        
      opt[Int]("txDelay")
        .text("Image folder location")
        .action((x, c) => c.copy(txDelay = x))
        .required()
        
      opt[Int]("numPartitions")
        .text("number of partitions")
        .action((x, c) => c.copy(numPartitions = x))
        .required()
    }
    
    parser.parse(args, KafkaProducerParam()).foreach { params =>
      //while(true) { 
        var prop = new Properties()
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.brokers)
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, params.clientId)
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      
        
        val producer = new KafkaProducer[String, String](prop)
        
        val dir = new File(params.imageFolder).listFiles
        var count = 0
        
        if(dir != null) {
          for(file <- dir) {
            try {
              
              val imageName = file.getName()
              
              println("Read from image path:");
              println(file.getAbsolutePath());
                    
              println("Image size:");
              println(Math.toIntExact(file.length()));
              
              val data: Array[Byte] = FileUtils.readFileToByteArray(file)
              
              if(data == null)
              {
                println("Error reading from file")
                break
              }
              
              var obj = new JsonObject()
              var gson = new Gson()
              obj.addProperty("origin", imageName)
              obj.addProperty("data", Base64.getEncoder.encodeToString(data))  
              
              val record: ProducerRecord[String, String] = new ProducerRecord(TOPIC, UUID.randomUUID.toString, gson.toJson(obj))
              producer.send(record)
              
              count = count + 1
              println("Image successfully sent " + count)
              
              Thread.sleep(params.txDelay);          
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
        
      //}
    }    
  }  
}