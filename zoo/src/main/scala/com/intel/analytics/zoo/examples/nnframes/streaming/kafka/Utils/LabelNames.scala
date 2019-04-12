package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{DataInputStream, FileInputStream}

import org.omg.CORBA.DynAnyPackage.InvalidValue

import scala.io.Source

object LabelNames {

  var labels: Array[String] = null


  def load(fileName: String) : Unit = {
    labels = Source.fromFile(fileName).getLines().toArray
  }

  def getLabel(className: Int) : String = {
    if(labels == null)
      throw new InvalidValue("Load labels file before trying to retrieve values")

    return labels(className - 1)
  }

  def printLabels(): Unit =
  {
    if(labels == null)
      throw new InvalidValue("Load labels file before trying to print values")

    labels.foreach(println)

  }

}
