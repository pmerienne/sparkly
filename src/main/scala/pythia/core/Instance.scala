package pythia.core

case class Instance(rawFeatures: Map[String, _] = Map(), inputMapper: Option[Mapper] = None, outputMapper: Option[Mapper] = None) {

  def inputFeature(name: String): Feature[Any] = {
    val realName = inputMapper.get.featureName(name)
    Feature(rawFeatures.get(realName).flatMap(Option(_)))
  }

  def inputFeature(name: String, value: Any): Instance = {
    val realName = inputMapper.get.featureName(name)
    copy(rawFeatures = rawFeatures + (realName -> value))
  }

  def inputFeatures(name: String): FeatureList = {
    val realNames = inputMapper.get.featuresNames(name)
    val values = realNames.map(realName => rawFeatures.get(realName))
    FeatureList(values.map(value => Feature(value.flatMap(Option(_)))))
  }

  def inputFeatures(name: String, values: List[_]): Instance = {
    val realNames = inputMapper.get.featuresNames(name)
    val newFeatures = (realNames zip values).toMap
    copy(rawFeatures = rawFeatures ++ newFeatures)
  }

  def outputFeature(name: String): Feature[Any] = {
    val realName = outputMapper.get.featureName(name)
    Feature(rawFeatures.get(realName).flatMap(Option(_)))
  }

  def outputFeature(name: String, value: Any): Instance = {
    val realName = outputMapper.get.featureName(name)
    copy(rawFeatures = rawFeatures + (realName -> value))
  }

  def outputFeatures(features: (String, _)*): Instance = {
    val realNames = features.map(_._1).map(outputMapper.get.featureName(_))
    val newFeatures = (realNames zip features.map(_._2)).toMap
    copy(rawFeatures = rawFeatures ++ newFeatures)
  }

  def outputFeatures(name: String, values:List[_]): Instance = {
    val realNames = outputMapper.get.featuresNames(name)
    val newFeatures = (realNames zip values).toMap
    copy(rawFeatures =rawFeatures ++ newFeatures)
  }

  def outputFeatures[T](name: String): FeatureList = {
    val realNames = outputMapper.get.featuresNames(name)
    val values = realNames.map(realName => rawFeatures.get(realName))
    FeatureList(values.map(value => Feature(value)))
  }

  def rawFeature(name: String): Feature[Any] = {
    Feature(rawFeatures.get(name).flatMap(Option(_)))
  }

}

object Instance {
  def apply(features: (String, _)*): Instance = new Instance(features.toMap)
}