package pythia.visualization

import java.net.InetAddress
import pythia.config.PythiaConfig
import org.jfarcand.wcs.{TextListener, WebSocket}
import pythia.core.VisualizationClient

object ConsoleVisualizationClient {

  def main(args: Array[String]) {
    val hostname = InetAddress.getLocalHost.getHostName
    val port = PythiaConfig.WEB_PORT
    val id = "pipeline-latency"
    val master = "local[4]"

    val url = new VisualizationClient(hostname, port, master, id).url
    WebSocket().open(url).listener(new TextListener {
      override def onMessage(msg: String) = println(msg)
    })
    println("Listening on " + url)
  }
}
