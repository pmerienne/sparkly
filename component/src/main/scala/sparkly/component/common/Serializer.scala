package sparkly.component.common

trait Serializer[T] extends Serializable {

  def serialize(value: T): Array[Byte]
  def deserialize(bytes: Array[Byte]): T
}
