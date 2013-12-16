package ohnosequences.nisperon.queues

import ohnosequences.nisperon.ProductMonoid

class ProductMessage[X, Y](mx: Message[X], my: Message[Y]) extends Message[(X, Y)] {

  //todo reading from product queue
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
class ProductQueue[X, Y](xQueue: MonoidQueue[X], yQueue: MonoidQueue[Y])
  extends MonoidQueue[(X, Y)](xQueue.name + "_" + yQueue.name, new ProductMonoid(xQueue.monoid, yQueue.monoid)) {


  //todo
  def list(): List[String] = List[String]()
  def read(id: String): Option[(X, Y)] = None

  def init() {
    xQueue.init()
    yQueue.init()
  }

  def put(id: String, values: List[(X, Y)]) {
    xQueue.put(id, values.map(_._1))
    yQueue.put(id, values.map(_._2))
  }

  def reset() {
    xQueue.reset()
    yQueue.reset()
  }

  //todo something with this block
  def read(): Message[(X, Y)] = {
    new ProductMessage(xQueue.read(), yQueue.read())
  }

  def isEmpty: Boolean = xQueue.isEmpty && yQueue.isEmpty

}

