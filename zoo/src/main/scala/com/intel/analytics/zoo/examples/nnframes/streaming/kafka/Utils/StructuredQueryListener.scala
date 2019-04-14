package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, PrintWriter}

import org.apache.spark.sql.streaming.StreamingQueryListener

class StructuredQueryListener(filePath: String) extends StreamingQueryListener {

  var fw: PrintWriter = _

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    fw = new PrintWriter(new File(filePath))

    println(s"Opened output file at location: ${filePath}")
    println("Query started: " + event.id)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    println("Query progress: " + event.progress)
    fw.println(event.progress.processedRowsPerSecond.toInt)
    fw.flush()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"Closed output file at location: ${filePath}")
    println("Query terminated: " + event.id)
    fw.close()
  }

}
