package pythia.config

import com.typesafe.config.ConfigFactory
import java.nio.file.{Paths, Path, Files}
import java.io.File

object PythiaConfig {

  val WEB_CONFIG = ConfigFactory.load().getConfig("web")
  val HOSTNAME = WEB_CONFIG.getString("hostname")
  val WEB_PORT = WEB_CONFIG.getInt("port")
  val WEB_SOURCES = WEB_CONFIG.getString("sources")

  val DATA_CONFIG = ConfigFactory.load().getConfig("data")
  val DATA_DIRECTORY = DATA_CONFIG.getString("directory")
  new File(DATA_DIRECTORY).mkdirs()

  val DB_DIRECTORY = DATA_DIRECTORY + "/db"
  new File(DB_DIRECTORY).mkdirs()

  val SPARK_CONFIG = ConfigFactory.load().getConfig("spark")
  val CHECKPOINTS_DIRECTORY = SPARK_CONFIG.getString("checkpoint-directory")
  new File(CHECKPOINTS_DIRECTORY).mkdirs()
  val METRICS_CONFIGURATION_FILE = SPARK_CONFIG.getString("metrics-conf")
}
