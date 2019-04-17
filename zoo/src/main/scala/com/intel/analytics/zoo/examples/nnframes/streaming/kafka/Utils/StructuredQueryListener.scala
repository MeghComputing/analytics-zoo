package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.StreamingQueryListener

class StructuredQueryListener(filePath: String) extends StreamingQueryListener {
  @transient lazy val logger = Logger.getLogger("meghlogger")
  var fw: PrintWriter = _

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    fw = new PrintWriter(new FileOutputStream(new File(filePath), true))

    logger.info(s"Opened output file at location: ${filePath}")
    logger.info("Query started: " + event.id)


  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    //logger.info("Query progress: " + event.progress)
    fw.println(event.progress.processedRowsPerSecond.toInt)
    fw.flush()
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logger.info(s"Closed output file at location: ${filePath}")
    logger.info("Query terminated: " + event.id)
    fw.close()
  }

}