package com.intel.analytics.zoo.examples.videoanalytics.structuredstreaming

import java.io.FileInputStream
import java.security.InvalidParameterException
import java.text.MessageFormat
import java.util.{Base64, Properties}

import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.nn.abstractnn.{AbstractModule, Activity}
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.image._
import com.intel.analytics.zoo.models.image.imageclassification.ImageClassificationConfig
import com.intel.analytics.zoo.pipeline.inference.FloatInferenceModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object StructuredStreamingConsumer extends Serializable {

  case class ImageInferenceParams(file: String = "", modelPath: String = "")

  val parser:OptionParser[ImageInferenceParams] = new OptionParser[ImageInferenceParams]("Image Inference") {
    head("Image Classification using Analytics Zoo")

    opt[String]("file")
      .text("Kafka property file")
      .action((x, c) => c.copy(file = x))
      .required()
    opt[String]("modelPath")
      .text("Image classification model path")
      .action((x, c) => c.copy(modelPath = x))
      .required()
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, ImageInferenceParams()).foreach { params =>
      var inference = new StructuredStreamingConsumer(params.file, params.modelPath)
      inference.readData()
    }
  }
}


class StructuredStreamingConsumer(file:String, modelPath:String) extends Serializable {
  val logger:Logger = LoggerFactory.getLogger(classOf[StructuredStreamingConsumer])

  val props = new Properties()
  try {
    val propFile = new FileInputStream(file)
    props.load(propFile)
  }catch {
    case e:Exception => logger.info("Error while reading property file")
  }

  val topicName:Set[String] = Set(props.getProperty("consumer.topic"))

  if (topicName == null) {
    throw new InvalidParameterException(MessageFormat.format("Missing value for kafka topic!"))
  }


  val schema = StructType(Seq(
    StructField("origin", DataTypes.StringType, true),
    StructField("data", DataTypes.StringType, true)
  ))


  val model:AbstractModule[Activity, Activity, Float] = Module.loadModule[Float](modelPath)
  val inferModel:FloatInferenceModel = new FloatInferenceModel(model.evaluate())


  def readData() {

    //Create DataSet from stream messages from kafka
    val conf:SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("Image Inference")
      .set("spark.streaming.receiver.maxRate", props.getProperty("spark.streaming.receiver.maxRate"))
      .set("spark.streaming.kafka.maxRatePerPartition", props.getProperty("spark.streaming.kafka.maxRatePerPartition"))
      .set("spark.shuffle.reduceLocality.enabled", props.getProperty("spark.shuffle.reduceLocality.enabled"))
      .set("spark.speculation", props.getProperty("spark.speculation"))

    val sc:SparkContext = NNContext.initNNContext(conf)

    val imageConfig = ImageClassificationConfig(props.getProperty("model.name"), "imagenet", "0.1") // needs to set model.name in prop file
    val transformer = BufferedImageResize(256, 256) ->
      ImageBytesToMat(imageCodec = 1) ->
      imageConfig.preProcessor ->
      ImageFeatureToTensor()

    val featureTransformersBC = sc.broadcast(transformer)
    val modelBroadCast = sc.broadcast(inferModel)

    val imgFEncoder = Encoders.bean(classOf[ImageFeature])

    val streamData = SQLContext.getOrCreate(sc).sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", props.getProperty("bootstrap.servers"))
      .option("subscribe", props.getProperty("kafka.topic"))
      .option("kafka.max.poll.records", props.getProperty("max.poll.records"))
      .load()
      .selectExpr("CAST(value AS STRING) as image")
      .select(from_json(col("image"), schema = schema).as("image"))
      .select("image.*")


    val predictImageUDF = udf((uri: String, data: String) => {
      try {
        val st = System.nanoTime()
        val featureSteps = featureTransformersBC.value.clonePreprocessing()
        val localModel = modelBroadCast.value
        val bytesData = Base64.getDecoder.decode(data)
        val imf = ImageFeature(bytesData, uri = uri)

        if (imf.bytes() == null)
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
          logger.error(e.toString)
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

    imageDF.show(5)
  }
}
