package ohnosequences.metapasta.reporting
import ohnosequences.nisperon.{intMonoid, Monoid}
import scala.collection.mutable


sealed trait AnyCellValue {
  type Type
  val value: Type

  val monoid: Monoid[Type]

  def mult(other: AnyCellValue): AnyCellValue.Of[Type]
}

object AnyCellValue {
  type Of[T] = AnyCellValue { type Type = T}
}

case class IntCellValue(value: Int, monoid: Monoid[Int]) extends AnyCellValue {

  type Type = Int

  override def toString: String = value.toString

  override def mult(other: AnyCellValue): AnyCellValue.Of[Int] = {
    val otherValue = other.value.asInstanceOf[Int]
    IntCellValue(monoid.mult(value, otherValue), monoid)
  }
}

case class StringCellValue(value: String, monoid: Monoid[String]) extends AnyCellValue {

  type Type = String

  override def toString: String = value
  override def mult(other: AnyCellValue): AnyCellValue.Of[String] = {
    val otherValue = other.value.asInstanceOf[String]
    StringCellValue(monoid.mult(value, otherValue), monoid)
  }
}


case class StringConstantMonoid(c: String) extends Monoid[String] {
  override def mult(x: String, y: String): String = c

  override def unit: String = c
}

trait AnyAttributeType {
  val name: String
  type Type
  val monoid: Monoid[Type]
  def wrap(v: Type): AnyCellValue.Of[Type]
  val default = wrap(monoid.unit)
}

case class IntAttribute(name: String, monoid: Monoid[Int]) extends AnyAttributeType {
  override type Type = Int

  override def wrap(v: Type): AnyCellValue.Of[Type] = IntCellValue(v, monoid)
}

case class StringAttribute(name: String, monoid: Monoid[String]) extends AnyAttributeType {
  override type Type = String

  override def wrap(v: Type): AnyCellValue.Of[Type] = StringCellValue(v, monoid)
}

case class CSVPrinter[A] (items: List[A], attributes: List[AnyAttributeType], f: (A, AnyAttributeType) => AnyCellValue) {

  def generate() = {

    // val initialTotal: List[AnyCell] = f(None)

    val total = new mutable.HashMap[AnyAttributeType, AnyCellValue]()
    for (at <- attributes) {
      total.put(at, at.default)
    }

    for (item <- items) {
      for (at <- attributes) {
        total.update(at, f(item, at).mult(total(at)))
      }
    }

    println(total)

  }
}

object Test {
  case class Item(name: String, age: Int)

  val items  = List(Item("n1", 44), Item("n2", 36))

  object name extends StringAttribute("name", new StringConstantMonoid("total"))
  object age extends IntAttribute("age", intMonoid)

  def f(item: Item, attribute: AnyAttributeType): AnyCellValue = {
    attribute match {
      case n : name.type => name.wrap(item.name)
      case a : age.type => age.wrap(item.age)
    }
  }

  def main(args: Array[String]) {
    val printer = new CSVPrinter(items, List(name, age), f)
    printer.generate()
  }



}
