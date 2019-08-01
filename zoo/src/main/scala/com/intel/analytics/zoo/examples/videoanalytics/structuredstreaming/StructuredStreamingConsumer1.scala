package com.intel.analytics.zoo.examples.videoanalytics.structuredstreaming

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Paths
import java.util.{Base64, Properties}
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.common.{NNContext, Utils}
import com.intel.analytics.zoo.feature.image.{ImageSet, _}
import com.intel.analytics.zoo.models.image.objectdetection.{ObjectDetector, Visualizer}
import com.intel.analytics.zoo.models.image.objectdetection.ObjectDetectionConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{Encoders, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

class StructuredStreamingConsumer1(prop: Properties) extends Serializable {

    // Megh logger
    @transient lazy val logger = Logger.getLogger("meghlogger")

    @transient var sc: SparkContext = _

    val outputFolder = "/home/bsharma/OutputObDetection/"

    // Structured streaming start
    def stream(): Unit = {
      logger.setLevel(Level.ALL)

      logger.info(s"Start DF Stream")
      // Set Spark config
      val conf = new SparkConf()
        .set(
          "spark.streaming.receiver.maxRate",
          prop.getProperty("spark.streaming.receiver.maxRate")
        )
        .set(
          "spark.streaming.kafka.maxRatePerPartition",
          prop.getProperty("spark.streaming.kafka.maxRatePerPartition")
        )
        .set(
          "spark.scheduler.minRegisteredResourcesRatio",
          prop.getProperty("spark.scheduler.minRegisteredResourcesRatio")
        )
        .set("spark.speculation", prop.getProperty("spark.speculation"))
        .set("spark.serializer", prop.getProperty("spark.serializer"))
        .set(
          "spark.executor.extraJavaOptions",
          prop.getProperty("spark.executor.extraJavaOptions")
        )
        .set(
          "spark.driver.extraJavaOptions",
          prop.getProperty("spark.driver.extraJavaOptions")
        )
        .set("spark.eventLog.enabled", prop.getProperty("spark.eventLog.enabled"))
        .set("spark.eventLog.dir", prop.getProperty("spark.eventLog.dir"))
        .set(
          "spark.history.fs.logDirectory",
          prop.getProperty("spark.history.fs.logDirectory")
        )
        .set("spark.executor.cores", prop.getProperty("spark.executor.cores"))
        .set(
          "spark.driver.maxResultSize",
          prop.getProperty("spark.driver.maxResultSize")
        )
        .set(
          "spark.shuffle.memoryFraction",
          prop.getProperty("spark.shuffle.memoryFraction")
        )
        .set("spark.network.timeout", prop.getProperty("spark.network.timeout"))
        .set("spark.app.name", prop.getProperty("spark.app.name"))
        .setMaster("local[*]")

      // Init Spark Context
      sc = NNContext.initNNContext(conf)
      // create schema for json message
      val schema = StructType(
        Seq(
          StructField("origin", DataTypes.StringType, true),
          StructField("data", DataTypes.StringType, true),
          StructField("latency", DataTypes.StringType, true)
        )
      )

      val imageConfig = ObjectDetectionConfig(prop.getProperty("model.name.detection"),
        "pascal",
        "0.1")

      val transformer =
        imageConfig.preProcessor

      Logger.getLogger("org").setLevel(Level.WARN)

      val model = ObjectDetector.loadModel[Float](
        prop.getProperty("model.full.path.detection"))
      val modelBroadCast = sc.broadcast(model)
      val featureTransformersBC = sc.broadcast(transformer)
//      val labelBroadcast = sc.broadcast(LabelNames.labels)

      // Create DataSet from stream messages from kafka
      val streamData = SQLContext
        .getOrCreate(sc)
        .sparkSession
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", prop.getProperty("bootstrap.servers"))
        .option("subscribe", prop.getProperty("kafka.topic"))
        .option("failOnDataLoss", "false")
        .option("auto.offset.reset", prop.getProperty("auto.offset.reset"))
        .option("maxOffsetsPerTrigger", prop.getProperty("maxOffsetsPerTrigger"))
        .load()
        .selectExpr("CAST(value AS STRING) as image")
        .select(from_json(col("image"), schema = schema).as("image"))
        .select("image.*")

      // User defined inference function
      val predictImageUDF = udf(
        (uri: String, data: Array[Byte], latency: String) => {
          try {
            val featureSteps = featureTransformersBC.value.clonePreprocessing()
            val st = System.nanoTime()
            //          val featureSteps = featureTransformersBC.value.clonePreprocessing()
            //          val localModel = modelBroadCast.value
            //          val labels = labelBroadcast.value

            val bytesData = Base64.getDecoder.decode(data)
            val imf = ImageFeature(bytesData, uri = uri)

            if (imf.bytes() == null) {
              "-2"
            }

           val rddImageFeature = sc.parallelize(Array(imf))

            val imgSet = ImageSet.rdd(rddImageFeature)
            val streamRead = ImageSet.streamread(imgSet)
//            val dataFinal = featureSteps(imgSet.array.iterator).next()
            val output = model.predictImageSet(streamRead)
            val endTime = (System.nanoTime() - st) / 1e9
            println("Detection takes " +endTime+ "sec")
            val cum_lat = (System.nanoTime() - st) / 1e6 + java.lang.Double
              .valueOf(latency)

            logger.info("ImageName: " + uri + "Cumulative latency: " + cum_lat)

            val visualizer = Visualizer(model.getConfig.labelMap, encoding = "jpg")
            val visualized = visualizer(output).toDistributed()
            val result = visualized.rdd.map(imageFeature =>
              (imageFeature.uri(), imageFeature[Array[Byte]](Visualizer.visualized))).collect()
            val outputTime = (System.nanoTime() - st) / 1e9
            println("Final detection takes " +outputTime)
                      result.foreach(x => {
                        Utils.saveBytes(x._2, getOutPath(outputFolder, x._1, "jpg"), true)
                      })
            logger.info(s"labeled images are saved to ${outputFolder}")
            "Found"

          } catch {
            case e: Exception =>
              logger.error(e)
              e.printStackTrace()
              "not found"
          }
        }: String
      )

      // Add prediction column = assign to udf o/p label
      val imageDF = streamData.withColumn(
        "prediction",
        predictImageUDF(col("origin"), col("data"), col("latency"))
      )

      // Init Structured Query Listener monitor
//      val queryMonitor = new StructuredQueryListener(
//        prop.getProperty("fps.out.file"),
//        prop.getProperty("multiplication.factor").toInt
//      )
//      SQLContext.getOrCreate(sc).sparkSession.streams.addListener(queryMonitor)

      var query: StreamingQuery = null
      var fos: FileOutputStream = null

      query = imageDF
            .selectExpr("origin", "prediction")
            .writeStream
            .outputMode("update")
            .format("console")
            .option("truncate", false)
            .start()

      query.awaitTermination()
      sc.stop()
      fos.close()
    }


    def getOutPath(outPath: String, uri: String, encoding: String): String = {
      Paths.get(outPath,
        s"detection_${ uri.substring(uri.lastIndexOf("/") + 1,
          uri.lastIndexOf(".")) }.${encoding}").toString
    }
  }


  /**
    * exec structured stream
    * parse input args
    */
  object StructuredStreamingConsumer1 {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("breeze").setLevel(Level.ERROR)
    Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)

    val logger = Logger.getLogger(getClass)
    logger.setLevel(Level.ALL)
    val parser =
      new OptionParser[TopNClassificationParam]("ImageClassification demo") {
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
        } catch {
          case e: Exception =>
            e.printStackTrace()
            sys.exit(1)
        }

//        LabelNames.load(prop.getProperty("label.file.path"))
        val sparkDriver = new StructuredStreamingConsumer1(prop)
        sparkDriver.stream()
      }
    }
    case class TopNClassificationParam(propFile: String = "")
  }



