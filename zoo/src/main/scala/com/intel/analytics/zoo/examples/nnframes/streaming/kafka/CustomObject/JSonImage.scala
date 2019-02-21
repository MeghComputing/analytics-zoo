package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.CustomObject

import sun.awt.image.ImageFetcher
import com.intel.analytics.bigdl.transform.vision.image.ImageFeature
import java.util.Base64

case class JSonImage(origin: String, height: Int, width: Int, nChannels: Int, mode: Int, data: String)


