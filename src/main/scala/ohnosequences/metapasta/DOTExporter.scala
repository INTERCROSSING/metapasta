package ohnosequences.metapasta

import java.io.{PrintWriter, File}
import org.clapper.avsl.Logger

object DOTExporter {

  val logger = Logger(this.getClass)

  def installGraphiz() {
    import scala.sys.process._

    logger.info("installing graphviz")
    "yum install -y graphviz".!
  }

  //1 [label="root", shape=box];
// b -- d;
  def generateDot(data: Map[String, TaxInfo], nodeRetriever: com.ohnosequences.bio4j.titan.model.util.NodeRetrieverTitan, dst: File) {
    logger.info("generating dot file: " + dst.getAbsolutePath)

    val result = new PrintWriter(dst)


    result.println("graph tax {")
    data.foreach { case (taxId, taxInfo) =>
      //print node

      if(taxId.equals("unassigned")) {
        result.println(taxId + "[label=\"" + taxId + "\\n" + taxInfo +  "\", shape=box];")
      } else {

        try {
          logger.info("taxInfo: " + taxInfo)
          val node = nodeRetriever.getNCBITaxonByTaxId(taxId)
          val name = node.getScientificName()
          result.println(taxId + " [label=\"" + name + "\\n" + taxInfo +  "\", shape=box];")
        } catch {
          case t: Throwable => logger.warn(t.toString)
          t.printStackTrace()
        }

        try {
          val node = nodeRetriever.getNCBITaxonByTaxId(taxId)
          val parent = node.getParent().getTaxId()
          result.println(parent + "--" + taxId + ";")
        } catch {
          case t: Throwable => logger.warn(t.toString)
            t.printStackTrace()
        }
      }
    }

    result.println("}")
    result.close()
  }

  def generatePdf(dot: File, pdf: File) {
    logger.info("generating pdf " + pdf.getAbsolutePath)
    val cmd = """dot -Tpdf -o$pdf$ $dot$"""
      .replace("$dot$", dot.getAbsolutePath)
      .replace("$pdf$", pdf.getAbsolutePath)
    import scala.sys.process._
    cmd.!
  }
}
