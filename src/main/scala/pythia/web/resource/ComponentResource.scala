package pythia.web.resource

import pythia.dao.ComponentRepository

class ComponentResource(implicit val componentRepository: ComponentRepository) extends BaseResource {

  get("/") {
    componentRepository.components()
  }

  get("/:id") {
    val id = params("id")
    componentRepository.component(id)
  }
}
