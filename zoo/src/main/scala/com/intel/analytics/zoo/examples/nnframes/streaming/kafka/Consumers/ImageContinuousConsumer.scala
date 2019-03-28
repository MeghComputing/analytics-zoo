package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Consumers

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.utils.LoggerFilter
import com.intel.analytics.bigdl.models.utils.ModelBroadcast
import com.intel.analytics.zoo.pipeline.nnframes._
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.image.{ImageMatToTensor, ImageSet, _}
import com.intel.analytics.bigdl.transform.vision.image.{ImageFeature, MatToFloats}
import com.intel.analytics.zoo.models.image.imageclassification.{ImageClassifier, LabelOutput, _}
import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Deserializers._
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.zoo.pipeline.inference.FloatInferenceModel

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
import scala.util.Properties
import java.util.Properties
import java.io.FileInputStream

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
import org.apache.spark.sql.streaming.Trigger
import org.opencv.core.{CvType, Mat}
import com.intel.analytics.bigdl.transform.vision.image.opencv.OpenCVMat
import org.opencv.imgcodecs.Imgcodecs

import java.util.Base64
import java.beans.Encoder


class ImageContinuousConsumer(prop: Properties) extends Serializable {
  @transient lazy val logger = Logger.getLogger("meghlogger")
  @transient var sc: SparkContext = _

  private val imageColumnSchema =
    StructType(StructField("image", NNImageSchema.byteSchema, true) :: Nil)

  private val TOPIC = Array(prop.getProperty("kafka.topic"))

  def stream() = {
    logger.setLevel(Level.ALL)

    logger.info(s"Start DF Stream")

    val conf = new SparkConf().set("spark.streaming.receiver.maxRate", prop.getProperty("spark.streaming.receiver.maxRate"))
      .set("spark.streaming.kafka.maxRatePerPartition", prop.getProperty("spark.streaming.kafka.maxRatePerPartition"))
      .set("spark.shuffle.reduceLocality.enabled", prop.getProperty("spark.shuffle.reduceLocality.enabled"))
      .set("spark.shuffle.blockTransferService", prop.getProperty("spark.shuffle.blockTransferService"))
      .set("spark.scheduler.minRegisteredResourcesRatio", prop.getProperty("spark.scheduler.minRegisteredResourcesRatio"))
      .set("spark.speculation", prop.getProperty("spark.speculation"))
      .setAppName(prop.getProperty("spark.app.name"))

    /*val kafkaConf = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> prop.getProperty("bootstrap.servers"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> prop.getProperty("max.poll.records"),
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> prop.getProperty("enable.auto.commit.config"),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> prop.getProperty("auto.offset.reset"),
      ConsumerConfig.GROUP_ID_CONFIG ->  prop.getProperty("group.id")
    )*/

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

    Logger.getLogger("org").setLevel(Level.WARN)

    val imageConfig = ImageClassificationConfig(prop.getProperty("model.name"), "imagenet", "0.1") // needs to set model.name in prop file
    val transformer = BufferedImageResize(256, 256) ->
      ImageBytesToMat(imageCodec = 1) ->
      imageConfig.preProcessor ->
      ImageFeatureToTensor()

    /*val transformer = BufferedImageResize(256, 256) ->
        ImageBytesToMat(imageCodec = 1) -> ImageCenterCrop(224, 224) ->
        ImageChannelNormalize(123, 117, 104) -> ImageMatToTensor() -> ImageFeatureToTensor()*/
    val featureTransformersBC = sc.broadcast(transformer)

    val model = Module.loadModule[Float](prop.getProperty("model.full.path"))
    val inferModel = new FloatInferenceModel(model.evaluate())
    val modelBroadCast = sc.broadcast(inferModel)

    //Create DataSet from stream messages from kafka
    val streamData = SQLContext.getOrCreate(sc).sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("bootstrap.servers"))
      .option("locationStrategy", "PreferBrokers")
      .option("subscribe", prop.getProperty("kafka.topic"))
      .option("kafka.max.poll.records", prop.getProperty("max.poll.records"))
      .load()
      .selectExpr("CAST(value AS STRING) as image")
      .select(from_json(col("image"),schema=schema).as("image"))
      .select("image.*")

    val predictImageUDF = udf ( (uri : String, data: Array[Byte]) => {
      try {
        val st = System.nanoTime()
        val featureSteps = featureTransformersBC.value.clonePreprocessing()
        val localModel = modelBroadCast.value

        val bytesData = Base64.getDecoder.decode(data)
        val imf = ImageFeature(bytesData, uri = uri)

        if(imf.bytes() == null)
          -2

        val imgSet = ImageSet.array(Array(imf))
        var inputTensor = featureSteps(imgSet.toLocal().array.iterator).next()
        inputTensor = inputTensor.reshape(Array(1) ++ inputTensor.size())
        val prediction = inferModel.predict(inputTensor).toTensor[Float].squeeze().toArray()
        val predictClass = prediction.zipWithIndex.maxBy(_._1)._2
        logger.info(s"transform and inference takes: ${(System.nanoTime() - st) / 1e9} s.")
        predictClass
      } catch {
        case e: Exception =>
          logger.error(e)
          e.printStackTrace()
          println(e)
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
      .trigger(Trigger.Continuous("1 seconds")) // 1 second is the checkpoint interval
      .start()

    query.awaitTermination()
    sc.stop()
  }
}

object ImageContinuousConsumer{
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("breeze").setLevel(Level.ERROR)
  Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)

  val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.ALL)

  case class TopNClassificationParam(propFile: String= "")

  val parser = new OptionParser[TopNClassificationParam]("ImageClassification demo") {
    head("Analytics Zoo ImageClassification demo")

    opt[String]("propFile")
      .text("properties files")
      .action((x, c) => c.copy(propFile = x))
      .required()
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, TopNClassificationParam()).foreach { params =>
      val prop = new Properties()

      try {
        prop.load(new FileInputStream(params.propFile))
      } catch { case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
      }

      val sparkDriver = new ImageContinuousConsumer(prop)
      sparkDriver.stream()
    }
  }
}

