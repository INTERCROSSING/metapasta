package ohnosequences.metapasta

import java.io.{PrintWriter, File}

object Utils {
  def writeFile(s: String, file: File) {
    val writer = new PrintWriter(file)
    writer.print(s)
    writer.close()
  }

  def readFile(file: File): String = {
    scala.io.Source.fromFile(file).mkString
  }

  def parseInt(s: String, default: Int = 0) = try {
    s.toInt
  } catch {
    case t: Throwable => default
  }

  def parseDouble(s: String, default: Double = 0D) = try {
    s.toDouble
  } catch {
    case t: Throwable => default
  }
}
