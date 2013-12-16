package ohnosequences.nisperon

trait Monoid[M] {

  def unit: M
  def mult(x: M, y: M): M
}

class ProductMonoid[X, Y](xMonoid: Monoid[X], yMonoid: Monoid[Y]) extends Monoid[(X, Y)] {
  def unit: (X, Y) = (xMonoid.unit, yMonoid.unit)

  def mult(x: (X, Y), y: (X, Y)): (X, Y) = (xMonoid.mult(x._1, y._1), yMonoid.mult(x._2, y._2))
}

class ListMonoid[T] extends Monoid[List[T]] {
  def unit: List[T] = List[T]()
  def mult(x: List[T], y: List[T]): List[T] = x ++ y
}

object intMonoid extends Monoid[Int] {
  def unit: Int = 0
  def mult(x: Int, y: Int): Int = x + y
}

object stringMonoid extends Monoid[String] {
  def unit = ""
  def mult(x: String, y: String): String = x + y
}



object unitMonoid extends Monoid[Unit] {
  def unit: Unit = ()
  def mult(x: Unit, y: Unit): Unit = ()
}





