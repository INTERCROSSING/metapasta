package ohnosequences.metapasta.scalaTests

import org.junit.Assert._
import org.junit.Test
import scala.util.{Failure, Try}

trait InitVals {
  def test = "name" + suffix

  def suffix: String

}

class A extends InitVals {
  override val suffix = "test"
}

object aa extends A


class B {
  @Test
  def test(): Unit = {
    assertEquals("nametest", aa.test)
  }
}
