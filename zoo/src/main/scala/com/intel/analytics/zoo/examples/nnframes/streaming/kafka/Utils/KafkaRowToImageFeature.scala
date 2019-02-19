package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import com.intel.analytics.zoo.feature.common.Preprocessing
import org.apache.spark.sql.Row

import com.intel.analytics.bigdl.tensor.{Storage, Tensor}
import com.intel.analytics.bigdl.transform.vision.image.opencv.OpenCVMat
import com.intel.analytics.zoo.feature.image.ImageSet
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.opencv.core.{CvType, Mat}
import org.opencv.imgcodecs.Imgcodecs

import java.util.Base64

import scala.language.existentials

import scala.reflect.ClassTag

/**
 * a Preprocessing that converts a Spark Row to a BigDL ImageFeature.
 */
class KafkaRowToImageFeature[T: ClassTag]()(implicit ev: TensorNumeric[T])
  extends Preprocessing[Row, ImageFeature] {

  override def apply(prev: Iterator[Row]): Iterator[ImageFeature] = {
    prev.map { row =>
      KafkaRow2IMF(row)
    }
  }
  
  def KafkaRow2IMF(row: Row): ImageFeature = {
    val imf = ImageFeature()
    imf.update(ImageFeature.uri, row.getString(0))
    val bytesData = Base64.getDecoder.decode(row.getAs[Array[Byte]](1))
    val opencvMat = OpenCVMat.fromImageBytes(bytesData)
    imf(ImageFeature.mat) = opencvMat
    
    imf
  }  
}

object KafkaRowToImageFeature {
  def apply[T: ClassTag]()(implicit ev: TensorNumeric[T]): KafkaRowToImageFeature[T] =
    new KafkaRowToImageFeature[T]()
}