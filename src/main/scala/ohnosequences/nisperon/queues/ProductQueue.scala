package ohnosequences.nisperon.queues

import ohnosequences.nisperon.{Monoid, Serializer, ProductMonoid}

//todo check it
class ProductSerializer[X, Y](x: Monoid[X], y: Monoid[Y]) extends Serializer[(X, Y)] {
  def fromString(s: String): (X, Y) = (x.unit, y.unit)

  def toString(t: (X, Y)): String = ""
}

class ProductMessage[X, Y](mx: Message[X], my: Message[Y]) extends Message[(X, Y)] {

  val id: String = mx.id + "," + my.id

  def value(): (X, Y) = (mx.value(), my.value())

  def delete() {
    mx.delete()
    my.delete()
  }

  def changeMessageVisibility(secs: Int) {
    mx.changeMessageVisibility(secs)
    my.changeMessageVisibility(secs)
  }
}

case class ProductQueue[X, Y](xQueue: MonoidQueue[X], yQueue: MonoidQueue[Y])
  extends MonoidQueue[(X, Y)](xQueue.name + "_" + yQueue.name, new ProductMonoid(xQueue.monoid, yQueue.monoid), new ProductSerializer[X, Y](xQueue.monoid, yQueue.monoid)) {

  def xId(id: String) = id + "._1"

  def isX(id: String) = {
    //todo it
  }

  def yId(id: String) = id + "._2"

  def list(): List[String] = xQueue.list() ++ yQueue.list()

  //todo it's wrong!
  def read(id: String): Option[(X, Y)] = {
    //val pairP = """"""


    xQueue.read(xId(id)) match {
      case Some(x) => Some(x -> yQueue.monoid.unit)
      case None => yQueue.read(yId(id)) match {
        case Some(y) => Some(xQueue.monoid.unit -> y)
        case None => None
      }
      case None => None
    }
//    (xQueue.read(xId(id)), yQueue.read(yId(id))) match {
//      case (Some(x), Some(y)) => Some(x, y)
//      case _ => None
//    }
//    (xQueue.read(id).getOrElse(xQueue.monoid.unit), yQueue.read(id).getOrElse(yQueue.monoid.unit)) match {
//      case (x, y) if x.equals(xQueue.monoid.unit) && y.equals(yQueue.monoid.unit) => None
//      case s => Some(s)
//    }
  }
  def delete(id: String) {
    xQueue.delete(xId(id))
    yQueue.delete(yId(id))
  }

  def initRead() {
    xQueue.initRead()
    yQueue.initRead()
  }

  def initWrite() {
    xQueue.initWrite()
    yQueue.initWrite()
  }

  def put(id: String, values: List[(X, Y)]) {
    xQueue.put(xId(id), values.map(_._1))
    yQueue.put(yId(id), values.map(_._2))
  }

  def reset() {
    xQueue.reset()
    yQueue.reset()
  }

  //todo reading from product queue
  def read(): Message[(X, Y)] = {
    new ProductMessage(xQueue.read(), yQueue.read())
  }

  def isEmpty: Boolean = xQueue.isEmpty && yQueue.isEmpty

}

