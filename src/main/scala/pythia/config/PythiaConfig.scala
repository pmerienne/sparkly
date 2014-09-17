package pythia.config

import com.typesafe.config.ConfigFactory
import java.nio.file.{Paths, Path, Files}
import java.io.File

object PythiaConfig {

  val WEB_CONFIG = ConfigFactory.load().getConfig("web")
  val WEB_PORT = WEB_CONFIG.getInt("port")
  val WEB_SOURCES = WEB_CONFIG.getString("sources")

  val DATA_CONFIG = ConfigFactory.load().getConfig("data")
  val DATA_DIRECTORY = DATA_CONFIG.getString("directory")
  new File(DATA_DIRECTORY).mkdirs()

  val DB_DIRECTORY = DATA_DIRECTORY + "/db"
  new File(DB_DIRECTORY).mkdirs()
}
