package pythia.model

case class TopologyConfig(id: String, name: String, components: List[ComponentConfig], connections: List[ConnectionConfig])
