package ohnosequences.nisperon

trait InstructionsAux {

  type I

  type O

  val arity: Int

  def solve(input: List[I]): List[O]
}

trait Instructions[Input, Output] extends InstructionsAux {

  def prepare()

  type I = Input

  type O = Output

}

abstract class MapInstructions[Input, Output] extends Instructions[Input, Output] {

  val arity = 1

  def solve(input: List[Input]): List[Output] = {
    List(apply(input.head))
  }

  def apply(input: Input): O

}

abstract class ReduceInstructions[Input, Output](val arity: Int) extends Instructions[Input, Output] {

  def solve(input: List[Input]): List[Output] = {
    List(apply(input))
  }

  def apply(input: List[Input]): Output

}

class MonoidReduceInstructions[Input](name: String, arity: Int, monoid: Monoid[Input]) extends ReduceInstructions[Input, Input](arity) {

  override def toString = name


  def prepare() {}

  def apply(input: List[Input]): Input = {
    input.fold(monoid.unit)(monoid.mult)
  }
}


abstract class SplitInstructions[Input, Output] extends Instructions[Input, Output] {

  val arity = 1

  def solve(input: List[Input]): List[Output] = {
    apply(input.head)
  }

  def apply(input: Input): List[Output]

}

class SplitterSplitInstructions[Input](name: String, splitter: Splitter[Input]) extends SplitInstructions[Input, Input] {


  def prepare() {}

  override def toString = name

  def apply(input: Input): List[Input] = splitter.split(input)
}

