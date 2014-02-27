package ohnosequences.nisperon

import ohnosequences.awstools.s3.ObjectAddress

trait InstructionsAux {

  type I

  type O

  val arity: Int

  def solve(input: List[I], logs: Option[ObjectAddress]): List[O]
}

trait Instructions[Input, Output] extends InstructionsAux {

  def prepare()

  type I = Input

  type O = Output

}

abstract class MapInstructions[Input, Output] extends Instructions[Input, Output] {

  val arity = 1

  def solve(input: List[Input], logs: Option[ObjectAddress]): List[Output] = {
    List(apply(input.head, logs))
  }

  def apply(input: Input, logs: Option[ObjectAddress]): O

}

abstract class ReduceInstructions[Input, Output](val arity: Int) extends Instructions[Input, Output] {

  def solve(input: List[Input], logs: Option[ObjectAddress]): List[Output] = {
    List(apply(input, logs))
  }

  def apply(input: List[Input], logs: Option[ObjectAddress]): Output

}

class MonoidReduceInstructions[Input](name: String, arity: Int, monoid: Monoid[Input]) extends ReduceInstructions[Input, Input](arity) {

  override def toString = name


  def prepare() {}

  def apply(input: List[Input], logs: Option[ObjectAddress]): Input = {
    input.fold(monoid.unit)(monoid.mult)
  }
}


abstract class SplitInstructions[Input, Output] extends Instructions[Input, Output] {

  val arity = 1

  def solve(input: List[Input], logs: Option[ObjectAddress]): List[Output] = {
    apply(input.head, logs)
  }

  def apply(input: Input, logs: Option[ObjectAddress]): List[Output]

}

class SplitterSplitInstructions[Input](name: String, splitter: Splitter[Input]) extends SplitInstructions[Input, Input] {


  def prepare() {}

  override def toString = name

  def apply(input: Input, logs: Option[ObjectAddress]): List[Input] = splitter.split(input)
}

