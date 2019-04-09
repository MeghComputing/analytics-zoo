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
import java.util.{Base64, Properties, UUID}
import com.google.gson.{Gson, JsonObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.opencv.core.{Core, Mat, MatOfByte}
import org.opencv.imgcodecs.Imgcodecs
import org.opencv.videoio.{VideoCapture, Videoio}
import org.slf4j.{Logger, LoggerFactory}
import scopt.OptionParser

/**
  * [[ImageProducerStructured]] Image producer object for parsing command line arguments and
  * calling video to image function for extracting image frames
  * from input video and ingesting it into kafka topic.
  */

object ImageProducerStructured {

  case class ImageProducerParams(file: String = "", inputVideo: String = "")

  val parser: OptionParser[ImageProducerParams] = new OptionParser[ImageProducerParams](
    "Image Producer") {
    head("Image producer with OpenCV and Kafka")
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

/**
  * Sets the Kafka topic name and loads properties file in
  * memory for intializing kafka producer.
  * @param file properties file for kafka and spark
  * @param input path to the input video
  */
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
  val topicName: String = properties.getProperty("kafka.topic")

  if (topicName == null) {
    throw new InvalidParameterException(MessageFormat.format("Missing value for kafka topic!"))
  }

  val producer = new KafkaProducer[String, String](properties)


  /**
    * Intializes the videocapture and starts capturing image frames
    * in the form of Mat object, converts into json object and sends
    * the data to kafka producer.
    */
  def videoToImages() {

    try {
      val capture = new VideoCapture()
      capture.open(input)

      if (!capture.isOpened) {
        logger.info("Error while capturing frames from video")
      }

      var frame_number = capture.get(Videoio.CAP_PROP_POS_FRAMES).toInt

      val mat = new Mat()

//      start capturing image frames
      while (capture.read(mat)) {
        val imageName = "IMAGE_" + frame_number + ".jpg"
        val byteMat = new MatOfByte()
        Imgcodecs.imencode(".jpg", mat, byteMat)
        val imageBytes = byteMat.toArray
//        convert the image byte array into string
        val data = Base64.getEncoder.encodeToString(imageBytes)
        var obj = new JsonObject()
        var gson = new Gson()
        obj.addProperty("origin", imageName)
        obj.addProperty("data", data)
        sendData(obj, gson)
        frame_number += 1
      }

      capture.release()
      mat.release()
      producer.close()
    } catch {
      case e: Exception => logger.info(e.toString)
        e.printStackTrace()
    }
  }

  /**
    * sends image data in the form of json string
    * to kafka topic.
    *
    * @param obj  JsonObject containing image data
    * @param gson Gson for converting json object into string
    */
  def sendData(obj: JsonObject, gson: Gson): Unit = {
    try {
      val record: ProducerRecord[String, String] = new ProducerRecord(topicName,
        UUID.randomUUID.toString,
        gson.toJson(obj))
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

