package com.datio.jobcustom

import com.datio.spark.InitSpark
import com.datio.spark.metric.model.BusinessInformation
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/**
  * Main file for ExampleSparkJob process.
  * Implements InitSpark which includes metrics and SparkSession.
  *
  * Configuration for this class should be expressed in HOCON like this:
  *
  * ExampleSparkJob {
  *   ...
  * }
  *
  */
protected trait ExampleSparkJobTrait extends InitSpark {
  this: InitSpark =>
    /**
      * @param spark Initialized SparkSession
      * @param config Config retrieved from args
      */
    override def runProcess(spark : SparkSession, config : Config): Int = {

      this.logger.info("Init process ExampleSparkJob")
      val rootConfig = config.getConfig("ExampleSparkJob")
      val rddQuijote = spark.sparkContext.textFile(rootConfig.getString("path"))
      val rddSplit = rddQuijote.flatMap(_.split(" "))
      val result = rddSplit.map(w => (w,1)).reduceByKey( (a,b) => a + b)
      result.foreach(println(_))
      this.logger.info("Conteo: " + rddSplit.count().toString)
      rddSplit.saveAsTextFile("/home/cgarrido/Documentos/Cesar/7.Scala/prueba3/output/part")

      val exitCode = 0
      exitCode

    }

  override def defineBusinessInfo(config: Config): BusinessInformation =
    BusinessInformation(exitCode = 0, entity = "", path = "", mode = "",
                        schema = "", schemaVersion = "", reprocessing = "")

}

object ExampleSparkJob extends ExampleSparkJobTrait with InitSpark

