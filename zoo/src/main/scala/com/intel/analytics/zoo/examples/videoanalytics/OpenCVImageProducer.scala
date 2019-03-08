package com.intel.analytics.zoo.examples.videoanalytics

import java.io.FileInputStream
import java.security.InvalidParameterException
import java.text.MessageFormat
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.opencv.core.{Core, Mat, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs
import org.opencv.videoio.VideoCapture
import org.opencv.videoio.Videoio
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser


object OpenCVImageProducer {

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
        var imageProducer = new OpenCVImageProducer(params.file, params.inputVideo)
        imageProducer.videoToImages()
    }
  }




  class OpenCVImageProducer(val file: String, val input: String) {
    val logger:Logger = LoggerFactory.getLogger(classOf[OpenCVImageProducer])

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


    val producer = new KafkaProducer[String, Array[Byte]](properties)

    def videoToImages(): Unit = {
      val cam = new VideoCapture()
      cam.open(input)

      val video_length = cam.get(Videoio.CAP_PROP_FRAME_COUNT).asInstanceOf[Int]
      val frames_per_second = cam.get(Videoio.CAP_PROP_FPS).asInstanceOf[Int]
//      var frame_number = cam.get(Videoio.CAP_PROP_POS_FRAMES).toInt

      logger.info("Number of frames " , +video_length)
      logger.info("Frames per second ", +frames_per_second)

      val mat = new Mat()

      if (cam.isOpened)
        {
          while (cam.read(mat)) {

            val byteMat = new MatOfByte()
            Imgcodecs.imencode(".jpg", mat, byteMat)
            val imageBytes = byteMat.toArray
            sendData(imageBytes, producer)
          }
          cam.release()
          mat.release()
        }

      closeProducer()

    }

    def sendData(data: Array[Byte], producer: KafkaProducer[String, Array[Byte]]): Unit = {
      val record = new ProducerRecord[String, Array[Byte]](topicName, data)
      try {
        producer.send(record)
      } catch {
        case e: Exception => logger.info("Error in sending record")
          e.printStackTrace()
        case e: InterruptedException => logger.info("Error in sending record to topic")
          e.printStackTrace()
          System.exit(1)
      }
    }

    def closeProducer(): Unit = {
      producer.flush()
      producer.close(100, TimeUnit.MILLISECONDS)
    }
  }
}