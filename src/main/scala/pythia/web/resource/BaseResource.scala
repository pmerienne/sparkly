package pythia.web.resource

import org.scalatra.ScalatraServlet
import org.scalatra.json.JacksonJsonSupport
import org.json4s.{DefaultFormats, Formats}
import pythia.web.model.ModelMapper

class BaseResource extends ScalatraServlet with JacksonJsonSupport {

  protected implicit val jsonFormats: Formats = DefaultFormats

  before() {
    contentType = formats("json")
  }

}
