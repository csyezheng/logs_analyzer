package apps.logs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OptionParser

import scala.reflect.runtime.universe._


/**
  * The LogAnalyzerAppMain is an sample logs analysis application.  For now,
  * it is a simple minimal viable product:
  *   - Read in new log files from a directory and input those new files into streaming.
  *   - Computes stats for all of time as well as the last time interval based on those logs.
  *   - Write the calculated stats to an txt file on the local file system
  * that gets refreshed every time interval.
  *
  * Once you get this program up and running, feed apache access log files
  * into the local directory of your choosing.
  *
  * Then open your output text file, perhaps in a web browser, and refresh
  * that page to see more stats come in.
  *
  * Modify the command line flags to the values of your choosing.
  * Notice how they come after you specify the jar when using spark-submit.
  *
  * Example command to run:
  * %  ${YOUR_SPARK_HOME}/bin/spark-submit
  * --class "com.databricks.apps.logs.LogAnalyzerAppMain"
  * --master spark://YOUR_SPARK_MASTER
  * target/scala-2.11/spark-logs-analyzer_2.11-2.0-assembly.jar
  * --logs-directory /tmp/logs
  * --output-html-file /tmp/log_stats.html
  * --window-length 30
  * --slide-interval 5
  * --checkpoint-directory /tmp/log-analyzer-streaming
  */
object LogAnalyzerAppMain {

  case class Params(
    windowLength: Int = 3000, 
    slideInterval: Int = 1000, 
    logsDirectory: String = "/tmp/logs",
    checkpointDirectory: String = "/tmp/checkpoint",
    outputHTMLFile: String = "/tmp/log_stats.html",
    outputDirectory: String = "/tmp/outpandas",
    indexHTMLTemplate :String ="./src/main/resources/index.html.template") extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LogAnalyzerAppMain") {
      head("LogAnalyzer", "0.1")
      opt[Int]('w', "window_length") 
        .text("size of the window as an integer in miliseconds")
        .action((x, c) => c.copy(windowLength = x))
      opt[Int]('s', "slide_interval") 
        .text("size of the slide inteval as an integer in miliseconds")
        .action((x, c) => c.copy(slideInterval = x))
      opt[String]('l', "logs_directory") 
        .text("location of the logs directory. if you don't have any logs use the fakelogs_dir script.")
        .action((x, c) => c.copy(logsDirectory = x))
      opt[String]('c', "checkpoint_directory") 
        .text("location of the checkpoint directory.")
        .action((x, c) => c.copy(checkpointDirectory = x))
      opt[String]('o', "output_directory") 
        .text("location of the output directory.")
        .action((x, c) => c.copy(outputDirectory = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    // Create Spark Conf and Spark Streaming Context.
    val sparkConf = new SparkConf().setAppName("A Databricks Reference Application: Logs Analysis with Spark")
    val streamingContext = new StreamingContext(sparkConf, Seconds(params.slideInterval))

    // Checkpointing must be enabled to use the updateStateByKey function.
    streamingContext.checkpoint(params.checkpointDirectory)

    // This methods monitors a directory for new files to read in for streaming.
    val logData: DStream[String] = streamingContext.textFileStream(params.logsDirectory)

    // Create DStream of Apache log entries
    val accessLogsDStream: DStream[ApacheAccessLog] = logData
      .flatMap(line => ApacheAccessLog.parseLogLine(line))
      .cache()

    // Process the DStream which gathers stats for all of time.
    val logAnalyzerTotal = new LogAnalyzerTotal()
    logAnalyzerTotal.processAccessLogs(accessLogsDStream)

    // Calculate statistics for the last time interval.
    val logAnalyzerWindowed = new LogAnalyzerWindowed(params.windowLength, params.slideInterval)
    logAnalyzerWindowed.processAccessLogs(accessLogsDStream)

    // Render the output each time there is a new RDD in the accessLogsDStream.
    val renderer = new Renderer(params.outputHTMLFile, params.windowLength)
    accessLogsDStream.foreachRDD(rdd =>
      renderer.render(logAnalyzerTotal.getLogStatistics, logAnalyzerWindowed.getLogStatistics))

    // Start the streaming server.
    streamingContext.start() // Start the computation
    streamingContext.awaitTermination() // Wait for the computation to terminate
  }

  abstract class AbstractParams[T: TypeTag] {
    private def tag: TypeTag[T] = typeTag[T]
    /**
      * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
      * {
      * [field name]:\t[field value]\n
      * [field name]:\t[field value]\n
      * ...
      * }
      */
    override def toString: String = {
      val tpe = tag.tpe
      val allAccessors = tpe.declarations.collect {
        case m: MethodSymbol if m.isCaseAccessor => m
      }
      val mirror = runtimeMirror(getClass.getClassLoader)
      val instanceMirror = mirror.reflect(this)
      allAccessors.map { f =>
        val paramName = f.name.toString
        val fieldMirror = instanceMirror.reflectField(f)
        val paramValue = fieldMirror.get
        s" $paramName:\t$paramValue"
      }.mkString("{\n", ",\n", "\n}")
    }
  }

}
