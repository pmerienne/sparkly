package sparkly.component.common

class Cache[T](fn: () => T) extends Serializable {

  private var value: Option[T] = None

  def get(): T = {
    value match {
      case Some(t) => t
      case None => {
        val t = fn()
        value = Some(t)
        t
      }
    }
  }
}
