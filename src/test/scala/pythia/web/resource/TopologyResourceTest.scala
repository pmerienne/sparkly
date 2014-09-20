package pythia.web.resource

class TopologyResourceTest extends ResourceSpec {
  addServlet(new PipelineResource(), "/topologies")

  "Topology resource" should "find topologies" in {
    get("/topologies") {
      status should equal(200)
      body should equal("[]")
    }

    get("/topologies/unknwown-topology") {
      status should equal(404)
    }

    put("/topologies", body = """{"id": "1", "name": "Test", "components": [], "connections": []}""") {
      status should equal(200)
    }

    get("/topologies/1") {
      status should equal(200)
      body should equal("""{"id":"1","name":"Test","components":[],"connections":[]}""")
    }

  }
}
