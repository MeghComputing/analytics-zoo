package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Consumers

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.utils.LoggerFilter
import com.intel.analytics.zoo.pipeline.nnframes._
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.image._
import com.intel.analytics.zoo.feature.image.ImageSet
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.models.image.imageclassification.{ImageClassifier, LabelOutput}
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Deserializers._

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


class ImageStructuredConsumer(module: String = "",
                     host: String = "",
                     port: Int = 9990,
                     nPartition: Int = 1,
                     batchSize: Int = 4
) extends Serializable {
  @transient lazy val logger = Logger.getLogger("meghlogger")
  @transient var sc: SparkContext = _  
  
  def doClassify(rdd : RDD[ImageFeature]) : DataFrame =  {
      var resultDF: DataFrame = null
    
      logger.info(s"Start classification")
      val notNullRDDs = rdd.filter(f => (f != null && f.bytes() != null ))
      val count = notNullRDDs.count()
      logger.info("RDD Count:" + count)
      if(count > 0)
      {
        logger.info(s"Non-Empty RDD start processing")
        //val data = ImageSet.rdd(rdd.coalesce(nPartition, true))    
        
        
        val getImageName = udf { row: Row => row.getString(0)}        
        val data = ImageSet.rdd(notNullRDDs)
        val mappedData = ImageSet.streamread(data, minPartitions = nPartition,
                          resizeH = 256, resizeW = 256, imageCodec = 1)
        val rowRDD = mappedData.toDistributed().rdd.map { imf => Row(NNImageSchema.imf2Row(imf))}
        val imageDF = SQLContext.getOrCreate(sc).createDataFrame(rowRDD, imageColumnSchema)
                    .repartition(nPartition)
                    .withColumn("imageName", getImageName(col("image")))
                    
        logger.info("#partitions: " + imageDF.rdd.partitions.length)
        logger.info("master: " + sc.master) 
        imageDF.cache().collect()

        val transformer = ImageCenterCrop(224, 224) ->
        ImageChannelNormalize(123, 117, 104) -> ImageMatToTensor() -> ImageFeatureToTensor()

        val model = Module.loadModule[Float](module)
        val dlmodel = NNClassifierModel(model, transformer)
                      .setBatchSize(batchSize)
                      .setFeaturesCol("image")
                      .setPredictionCol("prediction")
                      
        

        val st = System.nanoTime()
        resultDF = dlmodel.transform(imageDF)
        resultDF.collect()
        val time = (System.nanoTime() - st)/1e9
        logger.info("inference finished in " + time)
        logger.info("throughput: " + imageDF.count() / time)

      }
      
      resultDF
  }
  
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
    
    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", "50")
                  .set("spark.streaming.kafka.maxRatePerPartition", "50")
                  .set("spark.shuffle.reduceLocality.enabled", "false")
                  .set("spark.shuffle.blockTransferService", "nio")
                  .set("spark.scheduler.minRegisteredResourcesRatio", "1.0")
                  .set("spark.speculation", "true")
                  .setAppName("Image Streaming")
                  
    val kafkaConf = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> KAFKA_BROKERS,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> MAX_POLL_RECORDS,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> OFFSET_RESET_EARLIER,
      ConsumerConfig.GROUP_ID_CONFIG ->  GROUP_ID
    )
    
    //SparkSesion
    sc = NNContext.initNNContext(conf)
  	
  	//create schema for json message
    val schema = StructType(Seq(
      StructField("origin", DataTypes.StringType, true), 
      StructField("height", DataTypes.IntegerType, true), 
      StructField("width", DataTypes.IntegerType, true), 
      StructField("nChannels", DataTypes.IntegerType, true),
      StructField("mode", DataTypes.IntegerType, true), 
      StructField("data", DataTypes.StringType, true)  
    ))
    
    val getImageName = udf { row: Row => row.getString(0)} 
  	
  	/*  DataTypes.createStructType(Array(
  			DataTypes.createStructField("imageName", DataTypes.StringType, true),
  			DataTypes.createStructField("data", DataTypes.StringType, true)
  			));*/
    
    val transformer = KafkaRowToImageFeature() -> BufferedImageResize(256, 256) ->
                      ImageBytesToMat(imageCodec = 1) -> ImageCenterCrop(224, 224) ->
                      ImageChannelNormalize(123, 117, 104) -> ImageMatToTensor() -> ImageFeatureToTensor()

    val model = Module.loadModule[Float](module)
 
    val dlmodel = NNClassifierModel(model, transformer)
                  .setBatchSize(batchSize)
                  .setFeaturesCol("image")
                  .setPredictionCol("prediction")	

    //Create DataSet from stream messages from kafka
    val imageDF = SQLContext.getOrCreate(sc).sparkSession
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
      .repartition(nPartition)
      .withColumn("imageName", getImageName(col("image")))
 
    //logger.info("#partitions: " + imageDF.rdd.partitions.length)
    //logger.info("master: " + sc.master) 
    //imageDF.cache().collect()    

    val st = System.nanoTime()
    val resultDF = dlmodel.transform(imageDF)
    //resultDF.collect()
                        
    val queries = resultDF.select("imageName", "prediction")
                        .orderBy("imageName")
                        .writeStream
                        .format("console")
                        .start()
    val time = (System.nanoTime() - st)/1e9
    logger.info("inference finished in " + time)                                 
                 
    queries.awaitTermination()
    sc.stop()
   }
}

object ImageStructuredConsumer{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("breeze").setLevel(Level.ERROR)
  Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)

  val logger = Logger.getLogger(getClass)

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
