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
package com.intel.analytics.zoo.examples.nnframes.imageInference

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric.NumericFloat
import com.intel.analytics.bigdl.utils.LoggerFilter
import com.intel.analytics.zoo.pipeline.nnframes._
import com.intel.analytics.zoo.common.NNContext
import com.intel.analytics.zoo.feature.image._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import scopt.OptionParser

/**
  * Scala example for image classification inference with Caffe Inception model on Spark DataFrame.
  * Please refer to the readme.md in the same folder for more details.
  */
object ImageInferenceExample2 {
  LoggerFilter.redirectSparkInfoLogs()

  def main(args: Array[String]): Unit = {

    val defaultParams = Utils2.LocalParams()
    Utils2.parser.parse(args, defaultParams).foreach { params =>
      val sc = NNContext.initNNContext("ImageInference")

      val getImageName = udf { row: Row => row.getString(0)}
      val imageDF = NNImageReader.readImages(params.imagePath, sc, minPartitions=params.nPartition,
        resizeH = 256, resizeW = 256, imageCodec = 1)
        .repartition(params.nPartition)
        .withColumn("imageName", getImageName(col("image")))

      println("#partitions: " + imageDF.rdd.partitions.length)
      println("master: " + sc.master)
      imageDF.cache().collect()

      val transformer = RowToImageFeature() -> ImageCenterCrop(224, 224) ->
        ImageChannelNormalize(123, 117, 104) -> ImageMatToTensor() -> ImageFeatureToTensor()

      val model = Module.loadModule[Float](params.modelPath)
      val dlmodel = NNClassifierModel(model, transformer)
        .setBatchSize(params.batchSize)
        .setFeaturesCol("image")
        .setPredictionCol("prediction")

      val st = System.nanoTime()
      val resultDF = dlmodel.transform(imageDF)
      resultDF.collect()
      val time = (System.nanoTime() - st)/1e9
      println("inference finished in " + time)
      println("throughput: " + imageDF.count() / time)

      resultDF.select("imageName", "prediction").orderBy("imageName").show(10, false)
    }
  }
}

private object Utils2 {

  case class LocalParams(
                          modelPath: String = " ",
                          imagePath: String = " ",
                          nPartition: Int = 4,
                          batchSize: Int = 4
                        )

  val parser = new OptionParser[LocalParams]("BigDL Example") {
    opt[String]("modelPath")
      .text(s"modelPath")
      .action((x, c) => c.copy(modelPath = x))
    opt[String]("imagePath")
      .text(s"imagePath")
      .action((x, c) => c.copy(imagePath = x))
    opt[Int]('b', "batchSize")
      .text(s"batchSize")
      .action((x, c) => c.copy(batchSize = x))
    opt[Int]('p', "partition")
      .text("number of partitions")
      .action((x, c) => c.copy(nPartition = x))
      .required()
  }
}
