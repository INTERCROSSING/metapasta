package ohnosequences.logging

import ohnosequences.nisperon.AWS
import com.amazonaws.services.dynamodbv2.model.{PutItemRequest, AttributeValue, DeleteItemRequest}

/**
 * Created by Evdokim on 02.05.2014.
 * writer write hashes by 1000 items
 * deleter delete them
 * with deleter should be faster
 */
class Writer(aws: AWS, table: String) {

  val ddb = aws.ddb

  val n = 100

  def generateItem(h: Int, r: Int): java.util.Map[String, AttributeValue] = {
    val item = new java.util.HashMap[String, AttributeValue]()
    item.put("h", new AttributeValue().withN(h.toString))
   // val rand = math.random.toString
   // val t = System.currentTimeMillis().toString
   // val r =t + rand
    item.put("r", new AttributeValue().withS(r.toString))
    item
  }

  def write() {
    import concurrent._
    import concurrent.ExecutionContext.Implicits._

    for(h <- 1 to 10000) {
      future {
        println("uploading to hash " + h)
        val t1 = System.currentTimeMillis()
        for (i <- 1 to n) {
          val it = generateItem(h, i)
          var uploaded = false

          while (!uploaded) {
            try {
              ddb.putItem(new PutItemRequest()
                .withTableName(table)
                .withItem(it)
              )
              uploaded = true
            } catch {
              case t: Throwable => println(t.toString)
            }
          }
        }
        val t2 = System.currentTimeMillis()
        println("hash " + h + " written in " + (t2- t1))
      }
    }
    val s = Console.readLine()
  }

  def delete() {
    import concurrent._
    import concurrent.ExecutionContext.Implicits._

    for(h <- 1 to 10000) {
      future {
        println("deleting hash " + h)
        val t1 = System.currentTimeMillis()
        for (i <- 1 to n) {
        //  val it = generateItem(h, i)
          var deleted = false

          while (!deleted) {
            try {
              ddb.deleteItem(new DeleteItemRequest()
                .withTableName(table)
                .withKey(generateItem(h, i))
              )
              deleted = true
            } catch {
              case t: Throwable => println(t.toString)
            }
          }
        }
        val t2 = System.currentTimeMillis()
        println("hash " + h + " deleted in " + (t2- t1))
      }
    }
    val s = Console.readLine()


  }


}
