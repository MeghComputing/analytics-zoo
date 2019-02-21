package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.{ByteArrayOutputStream, File}

import java.util.UUID

import util.control.Breaks._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.io.FileUtils

import scopt.OptionParser

import org.apache.log4j.{Level, Logger}

import scala.actors.threadpool.ExecutionException

import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.apache.kafka.common.serialization.StringSerializer
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Serializers._

object ImageProducer {
  
  val TOPIC: String = "imagestream1"

  case class KafkaProducerParam(brokers: String = "",
                                clientId: String = "",
                                imageFolder: String = "",
                                topic: String = "",
                                txDelay: Int= 0)

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
  }
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("breeze").setLevel(Level.ERROR)
    Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)
    val logger = Logger.getLogger(getClass)
    
    parser.parse(args, KafkaProducerParam()).foreach { params =>      
      while(true) { 
        
        val producer = ImageProducerFactory.createProducer(params.brokers, params.clientId)
        
        /*if(!Files.notExists(Paths.get(args(0))))
        {
          throw new IllegalArgumentException("check command line arguments")
        }*/
        
        val dir = new File(params.imageFolder).listFiles
        var count = 0
        
        if(dir != null) {
          for(file <- dir) {
            try {
              
              val imageName = file.getName()
              
              logger.info("Read from image path:")
              logger.info(file.getAbsolutePath())
                    
              logger.info("Image size:")
              logger.info(Math.toIntExact(file.length()))
              
              val data = FileUtils.readFileToByteArray(file)                 
              
              if(data == null)
              {
                logger.info("Error reading from file")
                break
              }
              
              val imgFeature = new ImageFeature(data, uri = imageName)
              
              val record: ProducerRecord[String, ImageFeature] = new ProducerRecord(params.topic, UUID.randomUUID.toString, imgFeature)
              producer.send(record)
              
              count = count + 1
              logger.info("Image successfully sent " + count)
              
              Thread.sleep(params.txDelay)
            }
            catch {
              case e: ExecutionException =>
                logger.info("Error in sending record")
                e.printStackTrace()
              case e: InterruptedException =>
                logger.info("Error in sending record")
                e.printStackTrace()
            }        
          }
        }
        
        producer.close()
      
      }
    }
      
    
  }
  
}