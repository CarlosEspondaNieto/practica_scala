package com.datio.jobcustom

import com.datio.integration.utils.exec.RunSpark
import com.datio.integration.utils.read.KafkaMetricReader
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.avro.generic.GenericRecord
import org.scalatest.Matchers
import java.io
import scala.io.Source


class ExampleSparkJobSteps extends Matchers with ScalaDsl with EN {

  var configFilePathHDFS = ""
  var exit_code: Int = _


  Given("""^a config file path in HDFS (.*)$""") {
    (configFilePathHDFS: String) =>

      this.configFilePathHDFS = configFilePathHDFS
  }

  When("""^execute (.*) in Spark$""") {
    (mainClass: String) =>

      exit_code = RunSpark.exec(configFilePathHDFS, s"com.datio.jobcustom.${mainClass}")
      println("Exit code: " + exit_code)
  }
  Given("""create a file output"""){

    val file = scala.io.Source.fromFile ("/home/cgarrido/Documentos/Cesar/7.Scala/prueba3/output/part/part-00000").isEmpty
    for (line <- Source.fromFile("/home/cgarrido/Documentos/Cesar/7.Scala/prueba3/output/part/part-00000").getLines) {
      println(line)
    }
    println("The file is empty:  "+file)

 }

}

