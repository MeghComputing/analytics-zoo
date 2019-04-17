package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.streaming.StreamingQueryListener

class StructuredQueryListener(filePath: String) extends StreamingQueryListener {

  var fw: PrintWriter = _

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {


    println(s"Opened output file at location: ${filePath}")
    println("Query started: " + event.id)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    println("Query progress: " + event.progress)

    fw = new PrintWriter(new FileOutputStream(new File(filePath), true))
    fw.println(event.progress.processedRowsPerSecond.toInt)
    fw.flush()
    fw.close()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"Closed output file at location: ${filePath}")
    println("Query terminated: " + event.id)

  }

}
