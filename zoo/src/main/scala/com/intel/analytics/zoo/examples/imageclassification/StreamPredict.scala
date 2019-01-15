/*
 * Copyright 2018 Analytics Zoo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.analytics.zoo.examples.imageclassification

import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.models.image.imageclassification.{ImageClassifier, LabelOutput}
import com.intel.analytics.zoo.feature.image.ImageSet

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.SocketInputDStream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator
import org.apache.spark.api.java.function.Function

import org.apache.log4j.{Level, Logger}

import scopt.OptionParser

import java.io.InputStream
import java.io.File
import java.io.DataInputStream
import java.io.ByteArrayInputStream
import java.io.BufferedInputStream
import java.io.PrintWriter
import java.io.StringWriter

import org.apache.commons.io.FileUtils

import java.nio.ByteBuffer
import java.nio.ByteOrder

import java.awt.image.BufferedImage

import scala.tools.jline_embedded.internal.InputStreamReader
import scala.reflect.io.Streamable.Bytes


class StreamPredict(module: String = "",
                     host: String = "",
                     port: Int = 9990,
                     topN: Int = 5,
                     nPartition: Int = 1,
                     batchDuration: Int = 500
) extends Serializable { 
  
   @transient lazy val logger = Logger.getLogger("meghlogger")

  def bytesToImageObjects(is: InputStream) : Iterator[ImageFeature] = {
      val dis = new DataInputStream(is)  

      class ImageIterator extends Iterator[ImageFeature] with Serializable {
        private var gotNext = false
        private var nextValue: ImageFeature = _
        protected var finished = false
        val sw = new StringWriter
        
         def getNext():Unit = {
             //logger.info("Start get next");
             try {
               //logger.info("Start reading record");
               
               val nameSize = new Array[Byte](4);
    	         dis.readFully(nameSize, 0, 4);
               val strLen = ByteBuffer.wrap(nameSize).order(ByteOrder.BIG_ENDIAN).getInt();
    	         //logger.info("Image name length: " + strLen);
    
    	         if (strLen <= 0 || strLen > 28) {
    	             logger.info("Image file name size is null or invalid");
    	             finished = true;
    	             dis.close();
    	             return
    	         }
            
    	         val name = new Array[Byte](strLen);
    	         dis.readFully(name, 0, strLen);
               val imageName = new String(name)
               if (imageName == null || imageName.isEmpty()) {
    	             logger.info("Image filename is null or empty");
    	             finished = true;
    	             dis.close();
    	             return
    	         }
    						
               //logger.info("Image filename: " + imageName);                
               
    						
               val imgLen = new Array[Byte](4);
    	         dis.readFully(imgLen, 0, 4);
               val len = ByteBuffer.wrap(imgLen).order(ByteOrder.BIG_ENDIAN).getInt();
    	         //logger.info("Image size: " + len);
    
    	         if (len <= 0) {
    	             finished = true;
    	             dis.close();
    	             return
    	         }
    
    	         val data = new Array[Byte](len);
    	         dis.readFully(data, 0, len);
    	         
    	         try{
    	             
    	             nextValue = ImageFeature(data, uri = imageName)    
                   if (nextValue.bytes() == null) {
                       logger.info("Next value empty!!");
        	             finished = true;
        	             dis.close();
        	             return
        	         }
    	         }
    	         catch {               
                   case e: Exception => e.printStackTrace(new PrintWriter(sw))
                   finished = true;
                   dis.close();
                   logger.error(sw.toString())
               }
               
               //logger.info("Next value fine");
             }
             catch {
               
               case e: Exception => e.printStackTrace(new PrintWriter(sw))
               finished = true;
               logger.error(sw.toString())
             }
          //ogger.info("End get next");
          gotNext = true
        }  
      
        override def hasNext: Boolean = {
          //logger.info("Start hasNext");
          if (!finished) {
            if (!gotNext) {
              getNext()
              if (finished) {
                finished = true
                dis.close()
              }
            }
          }
          //logger.info("End hasNext");
          !finished
        }
      
        override def next(): ImageFeature = {
          //logger.info("Start next");
          if (finished) {
            throw new NoSuchElementException("End of stream")            
          }
          if(!gotNext)
            getNext()
          gotNext = false
          //logger.info("End next");
          nextValue
        }        
      }
      
      new ImageIterator
  }

  def stream() = {
    val sc = NNContext.initNNContext("Image Classification")
      
    val ssc = new StreamingContext(sc, new Duration(batchDuration))
    ssc.remember(new Duration(60000));
    
    logger.info(s"Load model and start socket stream")
    
    val model = ImageClassifier.loadModel[Float](module)      
    var imageDStream = ssc.socketStream(host, port, bytesToImageObjects, StorageLevel.MEMORY_AND_DISK_SER)
    
    imageDStream.foreachRDD(rdd => {
      logger.info(s"Start classification")
      
      //val newrdd = rdd.filter(_.opencvMat() != null)
      
      if(!rdd.isEmpty())
      {
        logger.info(s"Non-Empty RDD start processing")
        //val data = ImageSet.rdd(rdd.coalesce(nPartition, true))
        
        val data = ImageSet.rdd(rdd)
        var mappedData = ImageSet.streamread(data)
        mappedData = ImageSet.rdd(mappedData.toDistributed().rdd.coalesce(nPartition, true))
        println("Predict class: Partition number" + data.toDistributed().rdd.partitions.length)
        val output = model.predictImageSet(mappedData)
        val labelOutput = LabelOutput(model.getConfig.labelMap, "clses", "probs")
        val start = System.nanoTime()
        val result = labelOutput(output).toDistributed().rdd.collect
        val duration = (System.nanoTime() - start)/1e9

        logger.info("inference finished in " + duration)
        logger.info("throughput: " + rdd.count() / duration)

        logger.info(s"Prediction result")
        result.foreach(imageFeature => {
          logger.info(s"image : ${imageFeature.uri}, top ${topN}")
          val clsses = imageFeature("clses").asInstanceOf[Array[String]]
          val probs = imageFeature("probs").asInstanceOf[Array[Float]]
          for (i <- 0 until topN) {
            logger.info(s"\t class : ${clsses(i)}, credit : ${probs(i)}")
          }
        })
      }
    })
    
    ssc.start()  
    ssc.awaitTermination()
   }
}  


object StreamPredict{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("breeze").setLevel(Level.ERROR)
  Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)

  val logger = Logger.getLogger(getClass)

  case class TopNClassificationParam(model: String = "",
                                     host: String = "",
                                     port: Int = 9990,
                                     topN: Int = 5,
                                     nPartition: Int = 1,
                                     batchDuration: Int = 500)

  val parser = new OptionParser[TopNClassificationParam]("ImageClassification demo") {
    head("Analytics Zoo ImageClassification demo")
   
    opt[String]("host")
      .text("host ip to connect to")
      .action((x, c) => c.copy(host = x))
      .required()      
    opt[Int]("port")
      .text("port to connect to")
      .action((x, c) => c.copy(port = x))
      .required()
    opt[String]("model")
      .text("Analytics Zoo model")
      .action((x, c) => c.copy(model = x))
      .required()
    opt[Int]("topN")
      .text("top N number")
      .action((x, c) => c.copy(topN = x))
      .required()
    opt[Int]('p', "partition")
      .text("number of partitions")
      .action((x, c) => c.copy(nPartition = x))
      .required()
    opt[Int]('b', "batchDuration")
      .text("batch duration")
      .action((x, c) => c.copy(batchDuration = x))
      .required()
  }   

  def main(args: Array[String]): Unit = {
      parser.parse(args, TopNClassificationParam()).foreach { params =>
      var sparkDriver = new StreamPredict(params.model,
  				        params.host,
  				        params.port,
  				        params.topN,
  				        params.nPartition,
  				        params.batchDuration) 
      sparkDriver.stream()
    }
  }
}
  
  


