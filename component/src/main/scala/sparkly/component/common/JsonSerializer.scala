package sparkly.component.common

import org.json4s._
import org.json4s.jackson.Serialization

import scala.reflect._

class JsonSerializer[T <: AnyRef](implicit mf: Manifest[T]) extends Serializer[T] {

  override def serialize(value: T): Array[Byte] = {
    implicit val formats = DefaultFormats

    val json: String = Serialization.write(value)
    json.getBytes()
  }


  override def deserialize(bytes: Array[Byte]): T = {
    implicit val formats = DefaultFormats
    
    val json = new String(bytes)
    Serialization.read[T](json)
  }
}
