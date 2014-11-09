package pythia.dao

import java.lang.reflect.Modifier

import pythia.core._
import org.reflections.Reflections
import scala.collection.JavaConversions._

class VisualizationRepository(val visualizationBasePackage: String) {

  private val reflections = new Reflections(visualizationBasePackage)

  private val metadatas = reflections
    .getSubTypesOf(classOf[Visualization])
    .filter(clazz => !clazz.isInterface && !Modifier.isAbstract(clazz.getModifiers()))
    .map(clazz => (clazz.getName() , clazz.newInstance.metadata))
    .toMap

  def visualizations(): Map[String, VisualizationMetadata] = metadatas
  def visualization(name: String) = metadatas.get(name)
  def findByClassName(name: String) = metadatas(name)
}
