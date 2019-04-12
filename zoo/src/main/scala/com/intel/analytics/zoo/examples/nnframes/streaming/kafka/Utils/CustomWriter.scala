package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.sql.{ForeachWriter, Row}

import scala.util.Properties

class CustomWriter(filePath: String) extends ForeachWriter[Row]{

  var fw: FileWriter = _

  override def open(partitionId: Long, version: Long): Boolean = {
    println(s"Opened output file at location: ${filePath}")
    fw = new FileWriter(filePath, true)
    true
  }

  override def process(value: Row): Unit = {

    if(value != null){
      fw.write(value(0) + "\t" + value(1) + Properties.lineSeparator )
      fw.flush()
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    println(s"Closed output file at location: ${filePath}")
    fw.close()
  }
}
