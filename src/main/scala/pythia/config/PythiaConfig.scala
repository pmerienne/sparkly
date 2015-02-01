package pythia.config

import java.io.File

import com.typesafe.config.ConfigFactory

object PythiaConfig {

  val WEB_CONFIG = ConfigFactory.load().getConfig("web")
  val HOSTNAME = WEB_CONFIG.getString("hostname")
  val WEB_PORT = WEB_CONFIG.getInt("port")
  val WEB_SOURCES = WEB_CONFIG.getString("sources")

  val DATA_CONFIG = ConfigFactory.load().getConfig("data")
  val BASE_LOCAL_DIRECTORY = DATA_CONFIG.getString("base-local-directory")
  val BASE_DISTRIBUTED_DIRECTORY = DATA_CONFIG.getString("base-distributed-directory")

  val DB_DIRECTORY = new File(BASE_LOCAL_DIRECTORY, "db").getAbsolutePath
  val BASE_CHECKPOINTS_DIRECTORY = new File(BASE_DISTRIBUTED_DIRECTORY, "checkpoints").getAbsolutePath
  val BASE_STATES_DIRECTORY = new File(BASE_DISTRIBUTED_DIRECTORY, "states").getAbsolutePath
}
