package ohnosequences.metapasta

import java.io.File

import org.junit.Assert._
import org.junit.Test

import scala.util.{Failure, Try}

class Bio4jTest extends MetapastaTest {
  //@Test
  def bio4jTest(): Unit = {
    launch("bio4jTest", false) { case (logger2, aws2, loadingManager2) =>
      val workingDirectory = new File("test")
      workingDirectory.mkdir()
      Bio4j.bio4j201401.get(logger2, workingDirectory, loadingManager2).flatMap { bio4j =>
        Option(bio4j.nodeRetriever.getNCBITaxonByTaxId("9606")) match {
          case None => Failure(new Error("couldn't retrive taxon 9606"))
          case Some(taxon) => {
            Option(taxon.getParent) match {
              case None => Failure(new Error("couldn't retrive parent for taxon 9606"))
              case Some(pTaxon) => {
                Try {
                  assertEquals("9605", pTaxon.getTaxId())
                }
              }
            }
          }
        }
      }
    }
  }
}
