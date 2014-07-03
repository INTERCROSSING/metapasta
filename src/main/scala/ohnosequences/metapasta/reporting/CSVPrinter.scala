//package ohnosequences.metapasta.reporting
//import ohnosequences.nisperon.{AWS, intMonoid, Monoid}
//import scala.collection.mutable
//import ohnosequences.awstools.s3.ObjectAddress
//
//
//sealed trait AnyCellValue {
//  type Type
//  val value: Type
//
//  val monoid: Monoid[Type]
//
//  def mult(other: AnyCellValue): AnyCellValue.Of[Type]
//}
//
//object AnyCellValue {
//  type Of[T] = AnyCellValue { type Type = T}
//}
//
//case class IntCellValue(value: Int, monoid: Monoid[Int]) extends AnyCellValue {
//
//  type Type = Int
//
//  override def toString: String = value.toString
//
//  override def mult(other: AnyCellValue): AnyCellValue.Of[Int] = {
//    val otherValue = other.value.asInstanceOf[Int]
//    IntCellValue(monoid.mult(value, otherValue), monoid)
//  }
//}
//
//case class DoubleCellValue(value: Double, monoid: Monoid[Double]) extends AnyCellValue {
//
//  type Type = Double
//
//  override def toString: String = value.toString
//
//  override def mult(other: AnyCellValue): AnyCellValue.Of[Double] = {
//    val otherValue = other.value.asInstanceOf[Double]
//    DoubleCellValue(monoid.mult(value, otherValue), monoid)
//  }
//}
//
//case class StringCellValue(value: String, monoid: Monoid[String]) extends AnyCellValue {
//
//  type Type = String
//
//  override def toString: String = value
//  override def mult(other: AnyCellValue): AnyCellValue.Of[String] = {
//    val otherValue = other.value.asInstanceOf[String]
//    StringCellValue(monoid.mult(value, otherValue), monoid)
//  }
//}
//
//
//case class StringConstantMonoid(c: String) extends Monoid[String] {
//  override def mult(x: String, y: String): String = c
//
//  override def unit: String = c
//}
//
//trait AnyAttributeType {
//  val name: String
//  type Type
//  val monoid: Monoid[Type]
//  def wrap(v: Type): AnyCellValue.Of[Type]
//  val default = wrap(monoid.unit)
//}
//
//class IntAttribute(val name: String, val monoid: Monoid[Int]) extends AnyAttributeType {
//  override type Type = Int
//
//  override def wrap(v: Type): AnyCellValue.Of[Type] = IntCellValue(v, monoid)
//}
//
//class DoubleAttribute(val name: String, val monoid: Monoid[Double]) extends AnyAttributeType {
//  override type Type = Double
//
//  override def wrap(v: Type): AnyCellValue.Of[Type] = DoubleCellValue(v, monoid)
//}
//
//class StringAttribute(val name: String, val monoid: Monoid[String]) extends AnyAttributeType {
//  override type Type = String
//
//  override def wrap(v: Type): AnyCellValue.Of[Type] = StringCellValue(v, monoid)
//}
//
//case class CSVPrinter[A] (aws: AWS, items: Iterable[A], attributes: List[AnyAttributeType], f: (A, AnyAttributeType) => AnyCellValue) {
//
//  def generate(destination: ObjectAddress) = {
//
//    // val initialTotal: List[AnyCell] = f(None)
//
//    val total = new mutable.HashMap[AnyAttributeType, AnyCellValue]()
//    val line = new mutable.StringBuilder()
//    for (at <- attributes) {
//      total.put(at, at.default)
//    }
//    val result = new mutable.StringBuilder()
//
//    for (item <- items) {
//
//      for (at <- attributes) {
//        val v = f(item, at)
//
//        if (!line.isEmpty) line.append(";")
//        line.append(v.toString)
//
//        total.update(at, v.mult(total(at)))
//      }
//      result.append(line + "\n")
//      line.clear()
//    }
//
//    for (at <- attributes) {
//      if (!line.isEmpty) line.append(";")
//      line.append(total(at).toString)
//    }
//    result.append(line + "\n")
//    line.clear()
//
//    aws.s3.putWholeObject(destination, result.toString())
//   // println(result.toString())
//
//  }
//}
//
//object Test {
//  case class Item(name: String, age: Int)
//
//  val items  = List(Item("n1", 44), Item("n2", 36))
//
//  object name extends StringAttribute("name", new StringConstantMonoid("total"))
//  object age extends IntAttribute("age", intMonoid)
//
//  def f(item: Item, attribute: AnyAttributeType): AnyCellValue = {
//    attribute match {
//      case n : name.type => name.wrap(item.name)
//      case a : age.type => age.wrap(item.age)
//    }
//  }
//
//  def main(args: Array[String]) {
//    val raw = Map("test" -> 123, "test2" -> 77)
//
//    def transform(m: Map[String, Int]): Iterable[Item] = {
//      m.map{ case (name, age) => Item(name, age)}
//    }
//
//   // val printer = new CSVPrinter(transform(raw), List(name, age), f)
//   // printer.generate()
//  }
//
//
//
//}
