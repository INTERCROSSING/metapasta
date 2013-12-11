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

