package com.intel.analytics.zoo.examples.nnframes.streaming

import java.io.{ByteArrayOutputStream, File}

import java.util.UUID
import java.util.Base64
import java.util.Properties

import util.control.Breaks._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths


import org.apache.commons.io.FileUtils

import scopt.OptionParser

import org.apache.log4j.{Level, Logger}

import scala.actors.threadpool.ExecutionException

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.opencv.core.{CvType, Mat}

import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.io.DataOutputStream
import scala.util.control.Exception.Finally
import scala.util.control.Exception.Finally
import java.io.IOException


object ImageSocketProducer {  
  
  case class ImageProducerParam(host: String = "",
                                port: Int = 9990,
                                imageFolder: String = "",
                                topic: String = "",
                                txDelay: Int= 0,
                                numPartitions: Int=1)

  val parser = new OptionParser[ImageProducerParam]("Image Kafka Producer") {
    head("Image Kafka Producer demo")
   
    opt[String]("host")
      .text("host to connect to")
      .action((x, c) => c.copy(host = x))
      .required() 
      
    opt[Int]("port")
      .text("port to connect to")
      .action((x, c) => c.copy(port = x))
      .required()
      
    opt[String]("imageFolder")
      .text("Image folder location")
      .action((x, c) => c.copy(imageFolder = x))
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
    
    
    
    parser.parse(args, ImageProducerParam()).foreach { params =>    
      
      val sparkClientSocket = new ServerSocket(params.port, 1, InetAddress.getByName(params.host))
      sparkClientSocket.setSoTimeout(0)      
      
      while(true) { 
        try{
        
          val client = sparkClientSocket.accept()
          val out = new DataOutputStream(client.getOutputStream())
          
          val dir = new File(params.imageFolder).listFiles
          var count = 0
          
          if(dir != null) {
            for(file <- dir) {
              try {
                
                out.flush()
                out.writeInt(file.getName().length())
                
                logger.info("Image name size:")
                logger.info(file.getName().length())
                
                out.flush();
                out.write(file.getName().getBytes())
                out.flush();
                out.writeInt(Math.toIntExact(file.length()))
                
                logger.info("Image size:");
                logger.info(Math.toIntExact(file.length()))
                      
                out.flush();
                out.write(FileUtils.readFileToByteArray(file))                           
                
                count = count + 1
                logger.info("Image successfully sent " + count)
                
                Thread.sleep(params.txDelay)
              }
              catch {
                case e: SocketTimeoutException =>                
                  logger.info("Socket timed out!")                
                  e.printStackTrace()
                  out.close()
                  client.close()
                  break
                case e: IOException =>                
                  logger.info("IO error!")                
                  e.printStackTrace()
                  out.close()
                  client.close()
                  break
                case e: Exception =>                
                  logger.info("Unexplained error!")                
                  e.printStackTrace()
                  out.close()
                  client.close()
                  break
              }
            }
          }  
        }
        catch {
            case e: Exception =>                
              logger.info("Unexplained error!")                
              e.printStackTrace()
              break
          }
      }
    } 
  }
}