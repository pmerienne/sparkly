package pythia.dao

import pythia.core._
import org.reflections.Reflections
import scala.collection.JavaConversions._

class ComponentRepository(implicit val componentBasePackage: String) {

  private val reflections = new Reflections(componentBasePackage)

  private val metadatas = reflections
    .getSubTypesOf(classOf[Component])
    .map(clazz => (clazz.getName() , clazz.newInstance.metadata))
    .toMap

  def components(): Map[String, Metadata] = metadatas
  def component(name: String) = metadatas.get(name)

}
