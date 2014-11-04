import javax.servlet.ServletContext
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.LifeCycle
import org.scalatra.servlet.ScalatraListener
import pythia.config._
import PythiaConfig._
import pythia.web.resource._

class ScalatraBootstrap extends LifeCycle with Bindings {
  override def init(context: ServletContext) {
    context.mount(new PipelineResource, "/api/pipelines/*")
    context.mount(new ComponentResource, "/api/components/*")
    context.mount(new LocalClusterResource(), "/api/clusters/local/*")
    context.mount(new PipelineValidationResource(), "/api/pipeline-validation/*")
    context.mount(new VisualizationResource(), "/api/visualization/*")
  }
}

object Boot {
  def main(args: Array[String]) {
    val port = WEB_PORT

    val server = new Server(port)
    val context = new WebAppContext()

    context setContextPath "/"
    context.setResourceBase(WEB_SOURCES)
    context.addEventListener(new ScalatraListener)

    server.setHandler(context)
    server.start
    server.join
  }
}
