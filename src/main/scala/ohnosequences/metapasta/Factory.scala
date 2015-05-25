package ohnosequences.metapasta

import scala.util.Try

trait Factory[C, +T] {
  def build(ctx: C): Try[T]
}
