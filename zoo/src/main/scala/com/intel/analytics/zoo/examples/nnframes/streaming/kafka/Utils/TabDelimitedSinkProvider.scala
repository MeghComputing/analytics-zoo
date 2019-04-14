package com.intel.analytics.zoo.examples.nnframes.streaming.kafka.Utils

import java.io.{FileOutputStream, FileWriter, PrintWriter}

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OutputMode

case class TabDelimitedSink(
                             sqlContext: SQLContext,
                             parameters: Map[String, String],
                             partitionColumns: Seq[String],
                             outputMode: OutputMode
                           ) extends Sink {

  override def clone(): AnyRef = super.clone()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    val pw = new PrintWriter(new FileWriter("output.csv"))

    data.foreach(row => {
      pw.println(row(0) + "\t" + row(1))
    })

    pw.close()

  }
}

class TabDelimitedSinkProvider extends StreamSinkProvider with DataSourceRegister {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    TabDelimitedSink(sqlContext, parameters, partitionColumns, outputMode)

  }

  override def shortName(): String = "tabdelimited"
}
