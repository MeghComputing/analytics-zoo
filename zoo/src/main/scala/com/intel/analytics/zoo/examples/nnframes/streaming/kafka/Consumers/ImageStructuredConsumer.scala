package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Consumers

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.utils.LoggerFilter
import com.intel.analytics.bigdl.models.utils.ModelBroadcast
import com.intel.analytics.zoo.pipeline.nnframes._
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.image._
import com.intel.analytics.zoo.feature.image.ImageSet
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.models.image.imageclassification.{ImageClassifier, LabelOutput}
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Deserializers._
import com.intel.analytics.bigdl.tensor.Tensor

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.SocketInputDStream
//import org.apache.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.util.NextIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.commons.io.FileUtils

import java.io.InputStream
import java.io.File
import java.io.DataInputStream
import java.io.ByteArrayInputStream
import java.io.BufferedInputStream
import java.io.PrintWriter
import java.io.StringWriter

import java.nio.ByteBuffer
import java.nio.ByteOrder

import java.awt.image.BufferedImage

import java.util.UUID

import scopt.OptionParser

import scala.tools.jline_embedded.internal.InputStreamReader
import scala.reflect.io.Streamable.Bytes

import org.apache.log4j.{Level, Logger}

import org.opencv.core.{CvType, Mat}
import org.opencv.imgcodecs.Imgcodecs
import org.apache.spark.SparkConf
import java.security.Key
import org.apache.spark.sql.Encoders
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.CustomObject.JSonImage
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils._
import org.apache.spark.sql.Dataset
import org.opencv.core.{CvType, Mat}
import com.intel.analytics.bigdl.transform.vision.image.opencv.OpenCVMat
import org.opencv.imgcodecs.Imgcodecs

import com.intel.analytics.bigdl.utils.Engine

import java.util.Base64
import java.beans.Encoder


class ImageStructuredConsumer(module: String = "",
                     host: String = "",
                     port: Int = 9990,
                     nPartition: Int = 1,
                     batchSize: Int = 4
) extends Serializable {
  @transient lazy val logger = Logger.getLogger("meghlogger")
  @transient var sc: SparkContext = _  
  
  private val imageColumnSchema =
    StructType(StructField("image", NNImageSchema.byteSchema, true) :: Nil)
    
  private val KAFKA_BROKERS = "222.10.0.51:9092"
  private val GROUP_ID = "consumerGroup1"
  private val TOPIC = Array("imagestream1")
  private val MAX_POLL_RECORDS: Integer = 1
  private val OFFSET_RESET_EARLIER = "earliest"

  def stream() = {
    logger.setLevel(Level.ALL)
    
    logger.info(s"Start DF Stream")
                  
    val sc = NNContext.initNNContext("imageinfer")
    val spark = SparkSession
      .builder.config(sc.getConf)
      .config("spark.streaming.receiver.maxRate", "50")
      .config("spark.streaming.kafka.maxRatePerPartition", "50")
      .config("spark.shuffle.reduceLocality.enabled", "false")
      .config("spark.shuffle.blockTransferService", "nio")
      .config("spark.scheduler.minRegisteredResourcesRatio", "1.0")
      .config("spark.speculation", "false")
      .getOrCreate() 
                    
  	
  	//create schema for json message
    val schema = StructType(Seq(
      StructField("origin", DataTypes.StringType, true), 
      StructField("height", DataTypes.IntegerType, true), 
      StructField("width", DataTypes.IntegerType, true), 
      StructField("nChannels", DataTypes.IntegerType, true),
      StructField("mode", DataTypes.IntegerType, true), 
      StructField("data", DataTypes.StringType, true)  
    ))
    
    val transformer = BufferedImageResize(256, 256) ->
        ImageBytesToMat(imageCodec = 1) -> ImageCenterCrop(224, 224) ->
        ImageChannelNormalize(123, 117, 104) -> ImageMatToTensor() -> ImageSetToSample()
        
    val model = Module.loadModule[Float](module)
    val featureTransformersBC = sc.broadcast(transformer)
    val modelBroadCast = ModelBroadcast[Float]().broadcast(sc, model.evaluate())
    
    val imgFEncoder = Encoders.bean(classOf[ImageFeature])

    //Create DataSet from stream messages from kafka
    val streamData = spark
      .readStream     
      .format("kafka")      
      .option("kafka.bootstrap.servers", KAFKA_BROKERS)
      .option("subscribe", "imagestream1")
      .option("kafka.max.poll.records", MAX_POLL_RECORDS.toString())
      .load()
      .selectExpr("CAST(value AS STRING) as image")
      .select(from_json(col("image"),schema=schema).as("image"))
      .select("image.*")
      .as(Encoders.product[JSonImage])      
      //.repartition(nPartition)
     
    val predictImageUDF = udf ( (uri : String, data: Array[Byte]) => {
      try {
        val st = System.nanoTime()
        val featureSteps = featureTransformersBC.value.clonePreprocessing()
        val localModel = modelBroadCast.value()
        
        val bytesData = Base64.getDecoder.decode(data)
        val imf = ImageFeature(bytesData, uri = uri)
       
        if(imf.bytes() == null)
          -2
        
        val imgSet = ImageSet.array(Array(imf))
        val localImageSet = imgSet.transform(featureSteps)
        val prediction = localModel.predictImage(localImageSet.toImageFrame())
          .toLocal().array.map(_.predict()).head.asInstanceOf[Tensor[Float]].toArray()
        val predictClass = prediction.zipWithIndex.maxBy(_._1)._2
        logger.info(s"read, transform and inference takes: ${(System.nanoTime() - st) / 1e9} s.")
        predictClass
      } catch {
        case e: Exception => logger.error(e)
        e.printStackTrace()
        -1
      }
    })

    val imageDF = streamData.withColumn("prediction", predictImageUDF(col("origin"), col("data")))
    val query = imageDF
                .selectExpr("origin", "prediction")
                .writeStream
                .outputMode("update")        
                .format("console")
                .option("truncate", false)        
                .start() 
        
    query.awaitTermination()
    sc.stop()
   }
}

object ImageStructuredConsumer{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("breeze").setLevel(Level.ERROR)
  Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)

  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.ALL)

  case class TopNClassificationParam(model: String = "",
                                     host: String = "",
                                     port: Int = 9990,
                                     nPartition: Int = 1,
                                     batchSize: Int = 4)

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
    opt[Int]('p', "partition")
      .text("number of partitions")
      .action((x, c) => c.copy(nPartition = x))
      .required()
    opt[Int]('b', "batchSize")
      .text("batch size")
      .action((x, c) => c.copy(batchSize = x))
      .required()
  }   

  def main(args: Array[String]): Unit = {
      parser.parse(args, TopNClassificationParam()).foreach { params =>
      
      var sparkDriver = new ImageStructuredConsumer(params.model,
  				        params.host,
  				        params.port,
  				        params.nPartition,
  				        params.batchSize) 
      sparkDriver.stream()
    }
  }
}
