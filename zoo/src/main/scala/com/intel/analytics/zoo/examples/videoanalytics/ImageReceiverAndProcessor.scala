package com.intel.analytics.zoo.examples.videoanalytics

import java.io.FileInputStream
import java.security.InvalidParameterException
import java.text.MessageFormat
import java.util.Properties

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.nn.abstractnn.{AbstractModule, Activity}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.common.Preprocessing
import com.intel.analytics.zoo.feature.image._
import com.intel.analytics.zoo.pipeline.nnframes.{NNClassifierModel, NNImageSchema}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser



object ImageReceiverAndProcessor {

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
      var inference = new ImageReceiverAndProcessor(params.file, params.modelPath)
      inference.readData()
    }
  }
}



class ImageReceiverAndProcessor(file:String,modelPath:String) extends Serializable {
  val logger:Logger = LoggerFactory.getLogger(classOf[ImageReceiverAndProcessor])

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

  val conf:SparkConf = new SparkConf().setAppName("Image Inference")
  val sc:SparkContext = NNContext.initNNContext(conf)
  val ssc = new StreamingContext(sc, Duration(props.getProperty("batchDuration").toLong))
//  var imageID = sc.accumulator(0,"Image ID")
  val model:AbstractModule[Activity, Activity, Float] = Module.loadModule[Float](modelPath)

  val kafkaParams: Map[String, Object] = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> props.getProperty("bootstrap.servers"),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> props.getProperty("group.id"),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> props.getProperty("auto.offset.reset"),
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> props.getProperty("enable.auto.commit.config"),
    ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> props.getProperty("fetch.message.max.bytes")
  )

  private val imageColumnSchema =
    StructType(StructField("image", NNImageSchema.byteSchema, true) :: Nil)

  def classifyImage(rdd:RDD[ImageFeature]) {
    val count = rdd.count()
    val startTime = System.nanoTime()
    val transformer:Preprocessing[Row,Tensor[Float]] = RowToImageFeature() -> ImageCenterCrop(224, 224) ->
      ImageChannelNormalize(123, 117, 104) -> ImageMatToTensor() -> ImageFeatureToTensor()

    val dlmodel = NNClassifierModel(model, transformer)
      .setBatchSize(props.getProperty("batchSize").toInt)
      .setFeaturesCol("image")
      .setPredictionCol("prediction")

    try{
      val data = ImageSet.rdd(rdd)
      val getImageName = udf { row: Row => row.getString(0)}
      val mappedData = ImageSet.streamread(data,
        resizeH = 256, resizeW = 256, imageCodec = 1)
      val rowRDD = {
        mappedData.toDistributed().rdd.map { imf => Row(NNImageSchema.imf2Row(imf)) }
      }

      val imageDF = SQLContext.getOrCreate(sc).createDataFrame(rowRDD, imageColumnSchema)
        .repartition(props.getProperty("rdd.partition").toInt)
        .withColumn("imageName", getImageName(col("image")))

      val resultDF = dlmodel.transform(imageDF)
      resultDF.collect()

      val endTime = System.nanoTime()
      val totalTime = (endTime - startTime)/1e9
      logger.info("Total inference finished in " +totalTime)
      logger.info("Overall throughput " + count / totalTime)
      resultDF.select("imageName", "prediction").
              orderBy("imageName").
              show(false)

    }catch {
      case e:Exception => logger.info("Error in classifying image ", e)
        e.printStackTrace()
    }

  }


  def readData(): Unit = {

    try {
      val stream = KafkaUtils.
        createDirectStream[String, Array[Byte]](
        ssc,
        PreferConsistent,
        Subscribe[String, Array[Byte]](topicName, kafkaParams))

      stream.foreachRDD((kafkaRDD: RDD[ConsumerRecord[String, Array[Byte]]]) => {

        val kafkaRDDCount = kafkaRDD.count()
        if (kafkaRDDCount > 0) {
          var rdd = kafkaRDD.map(row => row.value())
          logger.info("Extracted RDD Count: " + rdd.count())
          val rddWithIndex = rdd.zipWithUniqueId()
          val imageRDD = rddWithIndex.map{element =>
            val imageName = "IMAGE_" + element._2 + ".jpeg"
            new ImageFeature(element._1, uri = imageName)
          }

          classifyImage(imageRDD)
        }
      })
    }catch {
      case e:InterruptedException => logger.info("Error in reading data from kafka topic")
      case e:Exception => logger.info("Error in reading data from kafka",e)
        e.printStackTrace()
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

