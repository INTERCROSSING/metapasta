package ohnosequences.metapasta.reporting.spreadsheeet

import ohnosequences.nisperon.{doubleMonoid, intMonoid, Monoid}
import scala.collection.mutable

trait AnyAttributeValue {
  type Type
  val value: Type
}

object AnyAttributeValue {
  type Of[T] = AnyAttributeValue { type Type = T }
}

class IntAttributeValue(val value: Int) extends AnyAttributeValue {
  type Type = Int

  override def toString: String = value.toString
}

class DoubleAttributeValue(val value: Double) extends AnyAttributeValue {
  type Type = Double

  override def toString: String = value.toString
}

trait Context {
  def get[A <: AnyAttribute](attribute: A, index: Int): Option[attribute.Value]
  def getTotal[A <: AnyAttribute](attribute: A): attribute.Value
  def set[A <: AnyAttribute](attribute: A, index: Int)(value: attribute.Value)
 // def setTotal[A <: AnyAttribute](attribute: A)(value: attribute.Value)
}

class ListContext(val attributes: List[AnyAttribute]) extends Context {

  val map = new mutable.HashMap[(String, Int), AnyAttributeValue]()

  val totals = new mutable.HashMap[String, AnyAttributeValue]()



//  override def setTotal[A <: AnyAttribute](attribute: A)(value: attribute.Value): Unit = {
//
//  }

  for (attribute <- attributes) {
    totals.put(attribute.name, attribute.wrap(attribute.monoid.unit))
  }

  override def set[A <: AnyAttribute](attribute: A, index: Int)(value: attribute.Value): Unit = {
    val cur = totals.get(attribute.name) match {
      case None => attribute.monoid.unit
      case Some(va) => {
        va.asInstanceOf[attribute.Value].value
      }
    }
    totals.put(attribute.name, attribute.wrap(attribute.monoid.mult(cur, value.value)))
    map.put((attribute.name, index), value)
  }

  override def getTotal[A <: AnyAttribute](attribute: A): attribute.Value = {
    attribute.wrap(totals.get(attribute.name) match {
      case None => attribute.monoid.unit
      case Some(va) => {
        va.asInstanceOf[attribute.Value].value
      }
    })
  }

  override def get[A <: AnyAttribute](attribute: A, index: Int): Option[attribute.Value] = {
    map.get((attribute.name, index)).map(_.asInstanceOf[attribute.Value])
  }
}



trait AnyAttribute {
  type Type

  type Value = AnyAttributeValue.Of[Type]
  val monoid: Monoid[Type]
  val name: String

  type Item

//  def apply(item: Item, index: Int, context: Context): Value = {
//    context.get(this, index) match {
//      case None => {
//        val newVal: Value = execute(item, index, context)
//        context.set(this, index)(newVal)
//        newVal
//      }
//      case Some(value) => {
//        value
//      }
//    }
//  }

  def apply(context: Context): Value = {
    context.getTotal(this)
//    context.getTotal(this) match {
//      case None => {
//        //calculate total:
//        println("wrong refer")
//        wrap(monoid.unit)
//      }
//      case Some(va) => va
//    }
  }

  def apply(item: Item, index: Int, context: Context): Value = {
    context.get(this, index) match {
      case None => {
        val newVal: Value = execute(item, index, context)
        context.set(this, index)(newVal)
        newVal
      }
      case Some(value) => {
        value
      }
    }
  }

  def wrap(value: Type): Value

  def execute(item: Item, index: Int, context: Context): Value

}

object AnyAttribute {
  type For[T] = AnyAttribute { type Item = T }
}

trait IntAttribute extends AnyAttribute {
  override type Type = Int

  override def wrap(value: Type): Value = {
    new IntAttributeValue(value)
  }
}

trait DoubleAttribute extends AnyAttribute {
  override type Type = Double

  override def wrap(value: Type): Value = {
    new DoubleAttributeValue(value)
  }
}

object Test {
  object Id extends IntAttribute {
    type Item = (Int, String)
    val name: String = "id"
    val monoid: Monoid[Int] = intMonoid
    def execute(item: Item, index: Int, context: Context): Value = {
      wrap(item._1)
    }
  }

  object Counter extends IntAttribute {
    type Item = (Int, String)
    val name: String = "counter"
    val monoid: Monoid[Int] = intMonoid
    def execute(item: Item, index: Int, context: Context): Value = {
      val prev: Int = if (index < 1) {
        0
      } else {
        context.get(Counter, index -1).map(_.value).getOrElse(0)
      }
      wrap(prev + 1)
    }
  }

  object Freq extends DoubleAttribute {
    type Item = (Int, String)
    val name: String = "freq"
    val monoid = doubleMonoid
    def execute(item: Item, index: Int, context: Context): Value = {
      val total: Int = Counter.apply(context).value

      wrap((Counter.apply(item, index, context).value + 0.0) / total)
    }
  }


  def main(args: Array[String]) {
    val items = List (
      (1, "one"), (2, "two"), (3, "thee")
    )

    val attributes = List(Id, Counter, Freq)
    val executor = new Executor(attributes, items)
    executor.execute()

   // Counter.
  }
}


class Executor[Item](attributes: List[AnyAttribute.For[Item]], items: Iterable[Item]) {
  def execute() {
    val context = new ListContext(attributes)

    for (attribute <- attributes) {
      var index = 0
      for (item <- items) {
        val res = attribute.apply(item, index, context)
        println(attribute.name + "[" + index + "] = " + res)
        index += 1
      }
      println(attribute.name + "[*] = " + context.getTotal(attribute))
    }

  }
}
