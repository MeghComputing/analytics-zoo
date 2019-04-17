package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, FileOutputStream, FileWriter, PrintWriter}

import org.apache.log4j.Logger
import org.apache.spark.sql.{ForeachWriter, Row}
import sun.security.tools.policytool.Prin

import scala.util.Properties

class CustomFileWriter(filePath: String) extends ForeachWriter[Row]{
  @transient lazy val logger = Logger.getLogger("meghlogger")



  override def open(partitionId: Long, version: Long): Boolean = {
    logger.info(s"Opened output file at location: ${filePath}")
    true
  }

  override def process(value: Row): Unit = {
    logger.info(s"Start file writer for batch#")
    @transient val fos = new FileOutputStream(new File(filePath), true)
    @transient val pw: PrintWriter = new PrintWriter(fos)
    try{
      if(value != null){
        pw.println(value(0) + "\t" + value(1) + "\n")
      }
    }
    catch {
      case e: Exception =>
        logger.info("Unexplained error!")
        logger.error(e.getMessage())
    }
    finally {
      logger.info(s"End file writer for batch#")
      pw.close()
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    logger.info(s"Closed output file at location: ${filePath}")
  }
}
