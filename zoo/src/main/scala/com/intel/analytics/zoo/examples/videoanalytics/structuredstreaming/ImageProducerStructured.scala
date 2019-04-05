package com.intel.analytics.zoo.examples.videoanalytics.structuredstreaming

import java.io.FileInputStream
import java.security.InvalidParameterException
import java.text.MessageFormat
import java.util.{Base64, Properties, UUID}
import com.google.gson.{Gson, JsonObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.opencv.core.{Core, Mat, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs
import org.opencv.videoio.{VideoCapture, Videoio}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

object ImageProducerStructured {

  case class ImageProducerParams(file: String = "", inputVideo: String = "")

  val parser:OptionParser[ImageProducerParams] = new OptionParser[ImageProducerParams]("Image Producer") {
    head("Image producer with Kafka and OpenCV")
    opt[String]("file")
      .text("properties file for kafka")
      .action((x, c) => c.copy(file = x))
      .required()
    opt[String]("inputVideo")
      .text("video path")
      .action((x, c) => c.copy(inputVideo = x))
      .required()
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, ImageProducerParams()).foreach { params =>
      var imageProducer = new ImageProducerStructured(params.file, params.inputVideo)
      imageProducer.videoToImages()
    }
  }
}


class ImageProducerStructured(val file: String, val input: String) {

  val logger: Logger = LoggerFactory.getLogger(classOf[ImageProducerStructured])

  try
  {
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
  } catch {
    case e: Exception => logger.info("Error while loading OpenCV Library")
  }

  val properties = new Properties()

  try {
    val propertyFile = new FileInputStream(file)
    properties.load(propertyFile)
  } catch {
    case e: Exception => logger.info("Error while reading property file")
  }
  val topicName:String = properties.getProperty("kafka.topic")

  if (topicName == null) {
    throw new InvalidParameterException(MessageFormat.format("Missing value for kafka topic!"))
  }

  val producer = new KafkaProducer[String, String](properties)


  def videoToImages() {

    try{
      val capture = new VideoCapture()
      capture.open(input)

      if (!capture.isOpened) {
        logger.info("Error while capturing frames from video")
      }

      var frame_number = capture.get(Videoio.CAP_PROP_POS_FRAMES).toInt

      val mat = new Mat()
      val startTime = System.currentTimeMillis()

      while (capture.read(mat)) {
        val imageName = "IMAGE_" + frame_number + ".jpg"
        val byteMat = new MatOfByte()
        Imgcodecs.imencode(".jpg", mat, byteMat)
        val imageBytes = byteMat.toArray
        val data = Base64.getEncoder.encodeToString(imageBytes)
        var obj = new JsonObject()
        var gson = new Gson()
        obj.addProperty("origin", imageName)
        obj.addProperty("data", data)
        sendData(obj,gson)
        frame_number += 1
      }

      val endTime = System.currentTimeMillis()
      val totalTime = endTime - startTime
      val timePerFrame = totalTime.toFloat / frame_number
      logger.info("Total time " + totalTime)
      logger.info("Time per single frame " + timePerFrame)
      logger.info("Frame number " + frame_number)
      capture.release()
      mat.release()
      producer.close()
    }catch {
      case e: Exception => logger.info(e.toString)
        e.printStackTrace()
    }
  }

  def sendData(obj:JsonObject,gson:Gson): Unit = {
    try {
    val record: ProducerRecord[String, String] = new ProducerRecord(topicName,UUID.randomUUID.toString, gson.toJson(obj))
    producer.send(record)
    } catch {
      case e: Exception => logger.info("Error in sending record")
        e.printStackTrace()
      case e: InterruptedException => logger.info("Error in sending record to topic")
        e.printStackTrace()
        System.exit(1)
    }
  }
}

