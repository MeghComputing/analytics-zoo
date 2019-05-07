package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Consumers

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.zoo.pipeline.nnframes._
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.image.{ImageSet, _}
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.models.image.imageclassification._
import com.intel.analytics.zoo.pipeline.inference.{FloatModel, InferenceModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoders, ForeachWriter, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.StructField
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.{File, FileInputStream, FileOutputStream, FileWriter, IOException, PrintWriter}
import java.net.SocketTimeoutException

import org.apache.spark.SparkContext
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import java.util.{Base64, Properties}

import com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.util.control.Breaks.break


class ImageStructuredConsumer(prop: Properties) extends Serializable {
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
      .set("spark.serializer", prop.getProperty("spark.serializer"))
      .set("spark.executor.extraJavaOptions", prop.getProperty("spark.executor.extraJavaOptions"))
      .set("spark.driver.extraJavaOptions", prop.getProperty("spark.driver.extraJavaOptions"))
      .set("spark.eventLog.enabled", prop.getProperty("spark.eventLog.enabled"))
      .set("spark.eventLog.dir", prop.getProperty("spark.eventLog.dir"))
      .set("spark.history.fs.logDirectory", prop.getProperty("spark.history.fs.logDirectory"))
      .set("spark.executor.cores", prop.getProperty("spark.executor.cores"))
      .set("spark.driver.maxResultSize", prop.getProperty("spark.driver.maxResultSize"))
      .set("spark.shuffle.memoryFraction", prop.getProperty("spark.shuffle.memoryFraction"))
      .set("spark.network.timeout", prop.getProperty("spark.network.timeout"))
      .set("spark.app.name", prop.getProperty("spark.app.name"))
        .set("spark.sql.streaming.metricsEnabled", "true")

    sc = NNContext.initNNContext(conf)
    //create schema for json message
    val schema = StructType(Seq(
      StructField("origin", DataTypes.StringType, true),
      StructField("data", DataTypes.StringType, true)
    ))

    Logger.getLogger("org").setLevel(Level.WARN)
    val imageConfig = ImageClassificationConfig(prop.getProperty("model.name"), "imagenet", "0.1") // needs to set model.name in prop file
    val transformer = BufferedImageResize(256, 256) ->
      ImageBytesToMat(imageCodec = 1) ->
      imageConfig.preProcessor ->
      ImageFeatureToTensor()
    val featureTransformersBC = sc.broadcast(transformer)

    //val model = Module.loadModule[Float](prop.getProperty("model.full.path"))
    //val inferModel = new FloatModel(model.evaluate())
    val inferModel = new InferenceModel(prop.getProperty("rdd.partition").toInt)
    val model = inferModel.doLoad(prop.getProperty("model.full.path"))
    val modelBroadCast = sc.broadcast(inferModel)
    val labelBroadcast = sc.broadcast(LabelNames.labels)

    //Create DataSet from stream messages from kafka
    val streamData = SQLContext.getOrCreate(sc).sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("bootstrap.servers"))
      .option("subscribe", prop.getProperty("kafka.topic"))
      .option("failOnDataLoss","false")
      .option("auto.offset.reset", prop.getProperty("auto.offset.reset"))
      .option("maxOffsetsPerTrigger", prop.getProperty("maxOffsetsPerTrigger"))
      .load()
      .selectExpr("CAST(value AS STRING) as image")
      .select(from_json(col("image"),schema=schema).as("image"))
      .select("image.*")

    val predictImageUDF = udf ( (uri : String, data: Array[Byte]) => {
      try {
        val st = System.nanoTime()
        val featureSteps = featureTransformersBC.value.clonePreprocessing()
        val localModel = modelBroadCast.value
        val labels = labelBroadcast.value

        val bytesData = Base64.getDecoder.decode(data)
        val imf = ImageFeature(bytesData, uri = uri)

        if(imf.bytes() == null)
          "-2"

        val imgSet: ImageSet = ImageSet.array(Array(imf))
        var inputTensor = featureSteps(imgSet.toLocal().array.iterator).next()
        inputTensor = inputTensor.reshape(Array(1) ++ inputTensor.size())
        //val prediction = localModel.predict(inputTensor).toTensor[Float].squeeze().toArray()
        val prediction = localModel.doPredict(inputTensor).toTensor[Float].squeeze().toArray()
        val predictClass = prediction.zipWithIndex.maxBy(_._1)._2

        if(predictClass < 0 || predictClass > (labels.length - 1))
          "unknown"

        val labelName: String = labels(predictClass.toInt).toString()

        labelName
      } catch {
        case e: Exception =>
          logger.error(e)
          e.printStackTrace()
          //println(e)
          "not found"
      }
    }: String)

    val imageDF = streamData.withColumn("prediction", predictImageUDF(col("origin"), col("data")))
    val queryMonitor = new StructuredQueryListener(prop.getProperty("fps.out.file"), prop.getProperty("multiplication.factor").toInt)
    SQLContext.getOrCreate(sc).sparkSession.streams.addListener(queryMonitor)

    var query: StreamingQuery = null
    var fos: FileOutputStream = null

    prop.getProperty("sink.writer") match {
      case "CustomFileWriter" =>

        fos = new FileOutputStream(new File(prop.getProperty("classification.out.file")), true)

        query = imageDF
          .selectExpr("concat(origin, '\t', prediction) as results")
          .writeStream
          .outputMode("update")
          .option("truncate", false)
          .option("checkpointLocation", prop.getProperty("checkpoint.location"))
          .foreachBatch((batchDS, batchId) => {
            logger.info(s"Start file sink for batch#: ${batchId}")

            Console.withOut(fos){
              try {
                val resultsList = batchDS.as(Encoders.STRING).collectAsList()
                resultsList.toArray.foreach(s => Console.println(s))
              }
              catch {
                case e: Exception =>
                  logger.info(s"Unexplained error! in batch#${batchId}")
                  logger.error(e.getMessage())
              }
              finally {
                logger.info(s"End file sink for batch#: ${batchId}")
                fos.flush()
              }
            }
          })
          .start()

      case "ConsoleWriter" =>
        query = imageDF
          .selectExpr("origin", "prediction")
          .writeStream
          .outputMode("update")
          .format("console")
          .option("truncate", false)
          .start()
    }

    query.awaitTermination()
    sc.stop()
    fos.close()
  }
}

object ImageStructuredConsumer{
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

      LabelNames.load(prop.getProperty("label.file.path"))

      val sparkDriver = new ImageStructuredConsumer(prop)
      sparkDriver.stream()
    }
  }
}
