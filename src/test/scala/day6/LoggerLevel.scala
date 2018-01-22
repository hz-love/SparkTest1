package day6

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

object LoggerLevel extends Logging{
  def setStreamingLogLevels()= {
    val log4jInit: Boolean = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if(!log4jInit){
      logInfo("Setting log level to [WARN] for streaming example." +
        "To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
