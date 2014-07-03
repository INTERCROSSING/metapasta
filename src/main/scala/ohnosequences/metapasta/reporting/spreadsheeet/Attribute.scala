package ohnosequences.metapasta.reporting.spreadsheeet

import ohnosequences.nisperon.{doubleMonoid, intMonoid, Monoid}
import scala.collection.mutable


trait Context {
  def get[A <: AnyAttribute](attribute: A, index: Int): attribute.Type
  def getTotal[A <: AnyAttribute](attribute: A): attribute.Type
  def set[A <: AnyAttribute](attribute: A, index: Int)(value: attribute.Type)
}

class ListContext(val attributes: List[AnyAttribute]) extends Context {

  val map = new mutable.HashMap[(String, Int), Any]()

  val totals = new mutable.HashMap[String, Any]()

  for (attribute <- attributes) {
    totals.put(attribute.name, attribute.monoid.unit)
  }

  override def set[A <: AnyAttribute](attribute: A, index: Int)(value: attribute.Type): Unit = {
    val cur = totals.getOrElse(attribute.name, attribute.monoid.unit).asInstanceOf[attribute.Type]
    totals.put(attribute.name, attribute.monoid.mult(cur, value))
    map.put((attribute.name, index), value)
  }

  override def getTotal[A <: AnyAttribute](attribute: A): attribute.Type = {
    totals.getOrElse(attribute.name, attribute.monoid.unit).asInstanceOf[attribute.Type]
  }

  override def get[A <: AnyAttribute](attribute: A, index: Int): attribute.Type = {
    map.getOrElse((attribute.name, index), attribute.monoid.unit).asInstanceOf[attribute.Type]
  }
}

case class StringConstantMonoid(c: String) extends Monoid[String] {
  override def mult(x: String, y: String): String = c

  override def unit: String = c
}

trait AnyAttribute {
  type Type

  val monoid: Monoid[Type]
  val name: String
  type Item

  def execute(item: Item, index: Int, context: Context): Type

}

object AnyAttribute {
  type For[T] = AnyAttribute { type Item = T }
}

abstract class IntAttribute[I](val name: String, val monoid: Monoid[Int]) extends AnyAttribute {
  override type Type = Int
  override type Item = I
}

abstract class DoubleAttribute[I](val name: String, val monoid: Monoid[Double]) extends AnyAttribute {
  override type Type = Double
  override type Item = I
}

abstract class StringAttribute[I](val name: String, val monoid: Monoid[String]) extends AnyAttribute {
  override type Type = String
  override type Item = I
}



case class Freq[I](a: IntAttribute[I]) extends DoubleAttribute[I](a.name + ".freq", doubleMonoid) {
  override def execute(item: Item, index: Int, context: Context) = {
    context.get(a, index).toDouble / context.getTotal(a)
  }
}

case class Sum[I](a: IntAttribute[I]*) extends IntAttribute[I](a.map(_.name).reduce { _ + "+" + _}, intMonoid) {
  override def execute(item: Item, index: Int, context: Context) = {
    a.map {context.get(_, index)}.reduce{_ + _}
  }
}

object Test {
  object Id extends StringAttribute[(Int, String)]("id", new StringConstantMonoid("total")) {
    override type Item = (Int, String)
    override  def execute(item: Item, index: Int, context: Context) = {
      item._1.toString
    }
  }

  object Name extends StringAttribute[(Int, String)]("name", new StringConstantMonoid("")) {
    override type Item = (Int, String)
    override  def execute(item: Item, index: Int, context: Context) = {
      item._2
    }
  }

  object Counter extends IntAttribute[(Int, String)]("counter", intMonoid) {
    override def execute(item: Item, index: Int, context: Context) = {
      context.get(Counter, index -1) + 1
    }
  }


  def main(args: Array[String]) {
    val items = List (
      (1, "one"), (2, "two"), (3, "three"), (123, "one-two-three")
    )

    val attributes = List[AnyAttribute.For[(Int, String)]](Id, Name, Counter, Sum(Counter, Counter), Freq(Counter))
    val executor = new CSVExecutor(attributes, items)
    println(executor.execute())

  }
}


class Executor[Item](attributes: List[AnyAttribute.For[Item]], items: Iterable[Item]) {
  def execute() {
    val context = new ListContext(attributes)

    for (attribute <- attributes) {
      var index = 0
      for (item <- items) {
        val res = attribute.execute(item, index, context)
        context.set(attribute, index)(res)
        println(attribute.name + "[" + index + "] = " + res)
        index += 1
      }
      println(attribute.name + "[*] = " + context.getTotal(attribute))
    }
  }
}

class CSVExecutor[Item](attributes: List[AnyAttribute.For[Item]], items: Iterable[Item], val separator: String = ";") {
  def execute(): String  = {
    val context = new ListContext(attributes)

    println("executing")
    for (attribute <- attributes) {
      var index = 0
      for (item <- items) {
        val res = attribute.execute(item, index, context)
        context.set(attribute, index)(res)
       // println(attribute.name + "[" + index + "] = " + res)
        index += 1
      }
     // println(attribute.name + "[*] = " + context.getTotal(attribute))
    }

    val lines = new mutable.StringBuilder()

    var index = 0
    val line = new mutable.StringBuilder()

    for (item <- items) {

      for (attribute <- attributes) {
        if(!line.isEmpty) {
          line.append(";")
        }
        line.append(context.get(attribute, index).toString)
      }
      lines.append(line.toString() + System.lineSeparator())
      line.clear()
      // println(attribute.name + "[" + index + "] = " + res)
      index += 1
    }
    for (attribute <- attributes) {
      if(!line.isEmpty) {
        line.append(";")
      }
      line.append(context.getTotal(attribute).toString)
    }
    lines.append(line.toString() + System.lineSeparator())
    lines.toString()
  }
}


