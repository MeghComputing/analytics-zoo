package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.{ByteArrayOutputStream, File}

import java.util.UUID

import util.control.Breaks._

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.io.FileUtils;

import scala.actors.threadpool.ExecutionException

import com.intel.analytics.bigdl.transform.vision.image.ImageFeature

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object ImageProducer {
  
  val TOPIC: String = "imagestream1"
  
  def main(args: Array[String]): Unit = {
  
    
    val producer = ImageProducerFactory.createProducer
    
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
          
          val imgFeature = new ImageFeature(data, uri = imageName)
          
          val record: ProducerRecord[String, ImageFeature] = new ProducerRecord(TOPIC, UUID.randomUUID.toString, imgFeature)
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