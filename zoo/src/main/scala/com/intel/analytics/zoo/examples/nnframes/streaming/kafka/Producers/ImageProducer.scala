package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.{ByteArrayOutputStream, File}

import java.util.UUID

import util.control.Breaks._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.io.FileUtils

import scopt.OptionParser

import scala.actors.threadpool.ExecutionException

import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

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
    
    if(args.length < 2)
    {
      println("Invalid number of arguments")
      return
    }
    
    parser.parse(args, KafkaProducerParam()).foreach { params =>      
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
            
            println("Read from image path:")
            println(file.getAbsolutePath())
                  
            println("Image size:")
            println(Math.toIntExact(file.length()))
            
            val data = FileUtils.readFileToByteArray(file)                 
            
            if(data == null)
            {
              println("Error reading from file")
              break
            }
            
            val imgFeature = new ImageFeature(data, uri = imageName)
            
            val record: ProducerRecord[String, ImageFeature] = new ProducerRecord(params.topic, UUID.randomUUID.toString, imgFeature)
            producer.send(record)
            
            count = count + 1
            println("Image successfully sent " + count)
            
            Thread.sleep(params.txDelay)
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
  
}