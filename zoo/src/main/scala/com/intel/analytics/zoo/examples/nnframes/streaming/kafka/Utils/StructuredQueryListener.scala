package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, FileWriter, PrintWriter}

import org.apache.spark.sql.streaming.StreamingQueryListener

import scala.util.Properties

class StructuredQueryListener(filePath: String) extends StreamingQueryListener {

  var fw: FileWriter = _

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    fw = new FileWriter(filePath, true)

    println(s"Opened output file at location: ${filePath}")
    println("Query started: " + event.id)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    println("Query progress: " + event.progress)
    fw.write(event.progress.processedRowsPerSecond.toInt + Properties.lineSeparator )
    fw.flush()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"Closed output file at location: ${filePath}")
    println("Query terminated: " + event.id)
    fw.close()
  }

}
