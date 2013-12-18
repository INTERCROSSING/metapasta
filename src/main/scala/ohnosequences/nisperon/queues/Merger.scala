package ohnosequences.nisperon.queues

import org.clapper.avsl.Logger


class Merger(queue: MonoidQueueAux) {

  val logger = Logger(this.getClass)

  def merge() = {

    queue.init()

    logger.info("retrieving messages from the queue " + queue.name)
    val ids = queue.list()
    var res = queue.monoid.unit

    var left = ids.size

    logger.info("merging " + left + " messages")
    ids.foreach { id =>
      left -= 1
      queue.read(id) match {
        case None => logger.error("message " + id + " not found")
        case Some(m) => res = queue.monoid.mult(res, m)
      }

      if (left % 100 == 0) {
         logger.info(left + " messages left")
      }

    }

    logger.info("merged")

    logger.info("writing result")
    queue.put("result", List(res))

    logger.info("deleting messages")


    left = ids.size
    ids.foreach { id =>
      left -= 1
      if (left % 100 == 0) {
        logger.info(left + " messages left")
      }
      try {
        queue.delete(id)
      } catch {
        case t: Throwable => logger.error("error during deleting message " + id)
      }
    }
    queue.reset()

  }


}
