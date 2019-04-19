package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Producers

import java.io.{File, FileInputStream}
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

    case class KafkaProducerParam(propFile: String= "")

    val parser = new OptionParser[KafkaProducerParam]("Image Kafka Producer") {
      head("Image Kafka Producer")
      opt[String]("propFile")
        .text("properties files")
        .action((x, c) => c.copy(propFile = x))
        .required()
    }
    
    parser.parse(args, KafkaProducerParam()).foreach { params =>
      while(true) {

        val prop = new Properties()

        try {
          prop.load(new FileInputStream(params.propFile))
        } catch { case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
        }

        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("bootstrap.servers"))
        prop.put(ProducerConfig.CLIENT_ID_CONFIG, prop.getProperty("client.id"))
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, prop.getProperty("key.serializer"))
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,  prop.getProperty("value.serializer"))
      
        
        val producer = new KafkaProducer[String, String](prop)
        
        val dir = new File(prop.getProperty("image.folder")).listFiles
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
              
              val record: ProducerRecord[String, String] = new ProducerRecord(prop.getProperty("topics"), UUID.randomUUID.toString, gson.toJson(obj))
              producer.send(record)
              
              count = count + 1
              println("Image successfully sent " + count)
              
              Thread.sleep(prop.getProperty("delay.in.ms").toInt);
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
}