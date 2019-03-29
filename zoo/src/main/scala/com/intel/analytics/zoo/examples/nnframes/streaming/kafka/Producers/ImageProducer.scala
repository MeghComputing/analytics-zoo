package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.File

import java.util.UUID

import util.control.Breaks._

import org.apache.commons.io.FileUtils

import scopt.OptionParser

import org.apache.log4j.{Level, Logger}

import scala.actors.threadpool.ExecutionException

import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.clients.producer.ProducerRecord

object ImageProducer {

  case class KafkaProducerParam(brokers: String = "",
                                clientId: String = "",
                                imageFolder: String = "",
                                topic: String = "",
                                txDelay: Int= 0,
                                numPartitions: Int=1,
                                numImages: Int=1)

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
      
    opt[Int]("numImages")
      .text("number of images ti send")
      .action((x, c) => c.copy(numImages = x))
      .required()
  }
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("breeze").setLevel(Level.ERROR)
    Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)
    val logger = Logger.getLogger(getClass)
    
    parser.parse(args, KafkaProducerParam()).foreach { params => 
      var count = 0
      while(count < params.numImages) { 
        
        val producer = ImageProducerFactory.createProducer(params.brokers, params.clientId, params.numPartitions)
    
        /*if(!Files.notExists(Paths.get(args(0))))
        {
          throw new IllegalArgumentException("check command line arguments")
        }*/
        
        val dir = new File(params.imageFolder).listFiles
        if(dir != null) {
          for(file <- dir) {
            try {
              
              val imageName = file.getName()
              
              //logger.info("Read from image path:")
              //logger.info(file.getAbsolutePath())
                    
              //logger.info("Image size:")
              //logger.info(Math.toIntExact(file.length()))
              
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
              //logger.info("Image successfully sent " + count)
              //System.out.println("Image successfully sent " + count)
              
              if(count % 1000 == 0)
              {
                logger.info("Number of images successfully sent: " + count)
              }
              
              if(count >= params.numImages)
              {
                logger.info("Sent all requested images. Count: " + count)
                break;
              }
              
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