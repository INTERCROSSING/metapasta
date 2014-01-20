package ohnosequences.nisperon.queues

import scala.collection.JavaConversions._


class VisibilityExtender[T](name: String) extends Thread("extender_" + name) {

  val messages = new java.util.concurrent.ConcurrentHashMap[String, SQSMessage[T]]()


  def addMessage(m: SQSMessage[T]) {
    messages.put(m.receiptHandle, m)
  }

  def clear() {
    messages.clear()
  }

  override def run() {

    while (true) {
      messages.values().foreach {
        m =>
          try {
            m.changeMessageVisibility(50)
          } catch {
            case t: Throwable => {
              println("warning: invalid id" + t.getLocalizedMessage)
              messages.remove(m.receiptHandle)
            }
          }
      }
      Thread.sleep(10 * 1000)
    }
  }
}