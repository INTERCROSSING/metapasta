package ohnosequences.logging

import ohnosequences.nisperon.AWS
import com.amazonaws.services.dynamodbv2.model._
import scala.collection.JavaConversions._

class LogConsole(aws: AWS, table1: String, table2: String) {

  val ddb = aws.ddb

  def getLastMessages(component: String, instance: String): List[Message] = {
    ddb.query(new QueryRequest()
      .withTableName(table2)
      .withSelect(Select.ALL_ATTRIBUTES)
      .withKeyConditions(Map("h" -> new Condition()
        .withComparisonOperator(ComparisonOperator.EQ)
        .withAttributeValueList(new AttributeValue().withS(component + ":" + instance))
    ))
    ).getItems.map { map =>
      Message(map.get("b").getS, map.get("t").getS)
    }.toList
  }


}
