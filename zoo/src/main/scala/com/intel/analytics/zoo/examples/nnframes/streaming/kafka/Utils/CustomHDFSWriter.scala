package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{File, FileWriter, PrintWriter}

import javax.security.auth.login.AppConfigurationEntry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.{ForeachWriter, Row}

import scala.util.Properties

class CustomHDFSWriter(filePath: String) extends ForeachWriter[Row]{

  var fos: FSDataOutputStream = _

  override def open(partitionId: Long, version: Long): Boolean = {
    println(s"Opened output file at location: ${filePath}")

    //Split file paths
    val uri = filePath.substring(0, filePath.indexOf(".com/") + 4)
    val file = filePath.substring(filePath.indexOf(".com/") + 4)

    println("The HDFS URI resolves to:" + uri)
    println("The filepath resolves to:" + file)

    val path = new Path(file)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    fos = fs.create(path)

    true
  }

  override def process(value: Row): Unit = {

    if(value != null){
      fos.write((value(0) + "\t" + value(1) + Properties.lineSeparator).getBytes() )
      fos.flush()
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    println(s"Closed output file at location: ${filePath}")
    fos.close()
  }
}

