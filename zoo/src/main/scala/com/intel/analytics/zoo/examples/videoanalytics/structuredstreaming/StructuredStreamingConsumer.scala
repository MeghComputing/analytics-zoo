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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser


/**
  * [[StructuredStreamingConsumer]] Structured streaming object for parsing command line
  * arguments and calling the read and inference data function
  */

object StructuredStreamingConsumer extends Serializable {

  case class ImageInferenceParams(file: String = "", modelPath: String = "")

  val parser: OptionParser[ImageInferenceParams] = new OptionParser[ImageInferenceParams](
    "Image Inference") {
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
      inference.readAndInferImage()
    }
  }
}


/**
  * Sets the Kafka topic name and loads properties file
  * and deep learning model in memory for future use in
  * image classification.
  * @param file properties file for kafka and spark configuration
  * @param modelPath path of the model to be loaded for inference
  */
class StructuredStreamingConsumer(file: String, modelPath: String) extends Serializable {
  val logger: Logger = LoggerFactory.getLogger(classOf[StructuredStreamingConsumer])

  val props = new Properties()

  try {
    val propFile = new FileInputStream(file)
    props.load(propFile)
  } catch {
    case e: Exception => logger.info("Error while reading property file")
  }

  val topicName: Set[String] = Set(props.getProperty("consumer.topic"))

  if (topicName == null) {
    throw new InvalidParameterException(MessageFormat.format("Missing value for kafka topic!"))
  }

// schema for initial dataframe
  val schema = StructType(Seq(
    StructField("origin", DataTypes.StringType, true),
    StructField("data", DataTypes.StringType, true)
  ))


  val model: AbstractModule[Activity, Activity, Float] = Module.loadModule[Float](modelPath)
  val inferModel: FloatInferenceModel = new FloatInferenceModel(model.evaluate())


  /**
    * reads the image data streams after intializing the spark context
    * and predicts category of the received image using
    * resnet-50 model already loaded in memory while initializing
    * the class.
    */

  def readAndInferImage() {

//    configuring spark
    val conf: SparkConf = new SparkConf()
      .setAppName("Image Inference")
      .set("spark.streaming.receiver.maxRate",
        props.getProperty("spark.streaming.receiver.maxRate"))
      .set("spark.streaming.kafka.maxRatePerPartition",
        props.getProperty("spark.streaming.kafka.maxRatePerPartition"))
      .set("spark.shuffle.reduceLocality.enabled",
        props.getProperty("spark.shuffle.reduceLocality.enabled"))
      .set("spark.speculation", props.getProperty("spark.speculation"))

    val sc: SparkContext = NNContext.initNNContext(conf)

    val imageConfig = ImageClassificationConfig(
      props.getProperty("model.name"), "imagenet", "0.1") // needs to set model.name in prop file

//    pipeline involving data preprocessing steps
    val transformer = BufferedImageResize(256, 256) ->
      ImageBytesToMat(imageCodec = 1) ->
      imageConfig.preProcessor ->
      ImageFeatureToTensor()

    /*
    * broadcast the transformer and model to
    * each and every executor.
    */
    val featureTransformersBC = sc.broadcast(transformer)
    val modelBroadCast = sc.broadcast(inferModel)


//    read image data streams from kafka topic
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


    /*
    * user define function for preprocessing data, converting input streams
    * into bigdl image feature and doing final prediction
    */
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


    /*
    * applying user define function on selected column origin and data
    * that comprises of image name and image data respectively
    * and save the prediction output in new appended prediction column
    */
    val imageDF = streamData.withColumn("prediction", predictImageUDF(col("origin"), col("data")))

//    start the streaming query
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
