package pythia.config

import pythia.dao.{ComponentRepository, TopologyRepository}

trait Bindings {

  implicit val componentBasePackage = "pythia"

  implicit val topologyRepository = new TopologyRepository()
  implicit val componentRepository = new ComponentRepository()
}
