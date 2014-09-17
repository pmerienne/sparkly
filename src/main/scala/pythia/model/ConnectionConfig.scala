package pythia.model

case class ConnectionConfig(from: ConnectionPoint, to: ConnectionPoint)
case class ConnectionPoint(component: String, stream: String)
