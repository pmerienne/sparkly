package sparkly.utils

case class TernaryOperator(b: Boolean) {
  def ?[X](t: => X) = new {
    def |(f: => X) = if(b) t else f
  }
}

object ScalaUtils {
  implicit def BooleanToTernaryOperator(b: Boolean) = TernaryOperator(b)
}
