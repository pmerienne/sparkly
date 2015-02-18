package pythia.core

import java.util.Date

import breeze.linalg.DenseVector
import com.github.nscala_time.time.Imports._
import org.joda.time.format.ISODateTimeFormat

import scala.reflect._

case class FeatureList(values: List[Feature[_]] = List()) {
  def asList = values
  def as[T : ClassTag] = values.map(_.as[T])
  def asRaw = values.map(_.value.getOrElse(null))
  def asArrayOf[T : ClassTag]  = as[T].toArray
  def asDenseVector = DenseVector(asArrayOf[Double])
}

case class Feature[U : ClassTag](value: Option[U]) {

  def isDefined: Boolean = value.isDefined && value.get != null
  def isEmpty: Boolean = value.isEmpty || value.get == null

  def or[V: ClassTag](other: V): V = value match {
    case Some(_) => as[V]
    case None => other
  }

  def as[V: ClassTag]: V = {
    try {
      convertTo[V]()
    } catch {
      case e: MatchError => throw new IllegalArgumentException(s"Unsupported feature conversion from ${classTag[U]} to ${classTag[V]}");
    }
  }

  private def convertTo[V : ClassTag]():V = if (isEmpty) {
    null.asInstanceOf[V]
  } else {
    val transformed = value.get match {
      case stringValue: String => fromString[V](stringValue)
      case doubleValue: Double => fromDouble[V](doubleValue)
      case intValue: Int => fromInt[V](intValue)
      case longValue: Long => fromLong[V](longValue)
      case dateValue: Date => fromDate[V](dateValue)
      case booleanValue: Boolean => fromBoolean[V](booleanValue)
    }
    transformed.asInstanceOf[V]
  }

  private def fromDouble[T: ClassTag](value:Double):T = {
    val transformed = classTag[T] match {
      case t if t == classTag[Double] => value
      case t if t == classTag[String] => value.toString
      case t if t == classTag[Int] => value.toInt
      case t if t == classTag[Long] => value.toLong
      // case t if t == classTag[Date] => DateTime.parse(value).toDate
      case t if t == classTag[Boolean] => value > 0.0
    }
    transformed.asInstanceOf[T]
  }

  private def fromString[T: ClassTag](value:String):T = {
    val transformed = classTag[T] match {
      case t if t == classTag[Double] => value.toDouble
      case t if t == classTag[String] => value
      case t if t == classTag[Int] => value.toInt
      case t if t == classTag[Long] => value.toLong
      case t if t == classTag[Date] => DateTime.parse(value, ISODateTimeFormat.dateTimeParser.withZoneUTC()).toDate
      case t if t == classTag[Boolean] => value match {
        case "true" => true
        case "false" => false
        case "1" => true
        case "0" => false
        case "-1" => false
      }
    }
    transformed.asInstanceOf[T]
  }

  private def fromInt[T: ClassTag](value:Int):T = {
    val transformed = classTag[T] match {
      case t if t == classTag[Double] => value.toDouble
      case t if t == classTag[String] => value.toString
      case t if t == classTag[Int] => value
      case t if t == classTag[Long] => value.toLong
      case t if t == classTag[Date] => new Date(value.toLong)
      case t if t == classTag[Boolean] => value > 0
    }
    transformed.asInstanceOf[T]
  }

  private def fromLong[T: ClassTag](value:Long):T = {
    val transformed = classTag[T] match {
      case t if t == classTag[Double] => value.toDouble
      case t if t == classTag[String] => value.toString
      case t if t == classTag[Int] => value.toInt
      case t if t == classTag[Long] => value
      case t if t == classTag[Date] => new Date(value.toLong)
      case t if t == classTag[Boolean] => value > 0
    }
    transformed.asInstanceOf[T]
  }

  private def fromDate[T: ClassTag](value:Date):T = {
    val transformed = classTag[T] match {
      // case t if t == classTag[Double] => value.toDouble
      case t if t == classTag[String] => new DateTime(value).toString(ISODateTimeFormat.dateTime.withZoneUTC())
      case t if t == classTag[Int] => value.getTime.toInt
      case t if t == classTag[Long] => value.getTime
      case t if t == classTag[Date] => value
      // case t if t == classTag[Boolean] => value
    }
    transformed.asInstanceOf[T]
  }

  private def fromBoolean[T: ClassTag](value:Boolean):T = {
    val transformed = classTag[T] match {
      case t if t == classTag[Double] => if(value){1.0}else{0.0}
      case t if t == classTag[String] => value.toString
      case t if t == classTag[Int] => if(value){1}else{0}
      case t if t == classTag[Long] => if(value){1.0}else{0}
      // case t if t == classTag[Date] => new Date(value.toLong)
      case t if t == classTag[Boolean] => value
    }
    transformed.asInstanceOf[T]
  }

}