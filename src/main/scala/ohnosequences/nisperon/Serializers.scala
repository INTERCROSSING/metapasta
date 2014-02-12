package ohnosequences.nisperon

trait Serializer[T] {
  def fromString(s: String): T

  def toString(t: T): String
}


class JsonSerializer[T](implicit mf: scala.reflect.Manifest[T]) extends Serializer[T] {

  def fromString(s: String): T = {
    JSON.extract[T](s)
  }

  def toString(t: T): String = {
    JSON.toJSON(t.asInstanceOf[AnyRef])
  }

}

object unitSerializer extends Serializer[Unit] {
  def fromString(s: String) = ()
  def toString(t: Unit) = ""
}

object intSerializer extends Serializer[Int] {

  def fromString(s: String): Int = s.toInt

  def toString(t: Int): String = t.toString
}

object stringSerializer extends Serializer[String] {
  def fromString(s: String): String = s

  def toString(t: String): String = t
}
