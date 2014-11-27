package pythia.config

import java.io.File

import com.typesafe.config.ConfigFactory

object PythiaConfig {

  val WEB_CONFIG = ConfigFactory.load().getConfig("web")
  val HOSTNAME = WEB_CONFIG.getString("hostname")
  val WEB_PORT = WEB_CONFIG.getInt("port")
  val WEB_SOURCES = WEB_CONFIG.getString("sources")

  val DATA_CONFIG = ConfigFactory.load().getConfig("data")
  val DB_DIRECTORY = DATA_CONFIG.getString("db-directory")
  new File(DB_DIRECTORY).mkdirs()
  val CHECKPOINTS_DIRECTORY = DATA_CONFIG.getString("checkpoint-directory")
  new File(CHECKPOINTS_DIRECTORY).mkdirs()
}
