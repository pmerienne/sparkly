package pythia.web.resource

import pythia.dao.TopologyRepository
import pythia.model.TopologyConfig

class TopologyResource(implicit val topologyRepository: TopologyRepository) extends BaseResource {

  get("/") {
    topologyRepository.all()
  }

  get("/:id") {
    val id = params("id")
    topologyRepository.get(id) match {
      case Some(topology) => topology
      case None => halt(404)
    }
  }

  delete("/:id") {
    val id = params("id")
    topologyRepository.delete(id)
  }

  put("/") {
    val topology = parsedBody.extract[TopologyConfig]
    topologyRepository.store(topology.id, topology)
  }

}