package ohnosequences.nisperon.console

import unfiltered.netty.Https
import unfiltered.response._
import unfiltered.request.{BasicAuth, Path, Seg, GET}
import unfiltered.netty.cycle.{Plan, SynchronousExecution}
import unfiltered.netty.{Secured, ServerErrorResponse}
import java.io.{FileInputStream, ByteArrayInputStream, File}
import unfiltered.Cycle
import unfiltered.response.ResponseString
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import collection.JavaConversions._


trait Users {
  def auth(u: String, p: String): Boolean
}

case class Auth(users: Users) {
  def apply[A, B](intent: Cycle.Intent[A, B]) =
    Cycle.Intent[A, B] {
      case req@BasicAuth(user, pass) if users.auth(user, pass) =>
        println(req.uri)
        Cycle.Intent.complete(intent)(req)
      case _ =>
        Unauthorized ~> WWWAuthenticate( """Basic realm="/"""")
    }
}


class ConsolePlan(name: String, users: Users, console: Console) extends Plan
with Secured // also catches netty Ssl errors
with SynchronousExecution
with ServerErrorResponse {
  val aws =  console.nisperon.aws
  val as =  console.nisperon.aws.as.as

  def intent = Auth(users) {
    case GET(Path("/")) => {

     // val main =  getClass.getResourceAsStream("/console/main.html")
      val main = new FileInputStream(new File("src/main/resources/console/main.html"))


      val mainPage = io.Source.fromInputStream(main).mkString
        .replace("$name$", name)
        .replace("@nisperos", console.nisperos().toString())
      HtmlCustom(mainPage)
    }

    case GET(Path("/shutdown")) => {
      Server.shutdown()
      ResponseString("ok")
    }

    case GET(Path(Seg("terminate" :: id ::  Nil))) => {
      try {
        aws.ec2.terminateInstance(id)
        ResponseString("""<div class="alert alert-success">terminated</div>""")
      } catch {
        case t: Throwable => ResponseString("""<div class="alert alert-danger">failed</div>""")
      }
    }

    case GET(Path(Seg("nispero" :: nispero :: "workerInstances" ::  Nil))) => {
      val nisperoAux = console.nisperon.nisperos(nispero)
      val workersGroup = "metatest"
      val workers = AWSTools.describeAutoScalingGroup(aws, workersGroup)
      ResponseString(console.workerInstances(workers).toString())
    }


    case GET(Path(Seg("nispero" :: nispero :: Nil))) => {
      val nisperoAux = console.nisperon.nisperos(nispero)
      val workersGroup = "metatest"
     // val workersGroup= nisperoAux.nisperoConfiguration.workersGroupName
      val r = as.describeAutoScalingGroups(new DescribeAutoScalingGroupsRequest()
       .withAutoScalingGroupNames(workersGroup)
      )
//      r.getNextToken match {
//        case null =>
//      }


      val autoscalingGroupInfo = r.getAutoScalingGroups.toList





      // val main =  getClass.getResourceAsStream("/console/main.html")
      val main = new FileInputStream(new File("src/main/resources/console/main.html"))


      val mainPage = io.Source.fromInputStream(main).mkString
        .replace("$name$", name)
        .replace("@nisperos", console.nisperos().toString())
        .replace("@workersProperties", console.workersProperties(autoscalingGroupInfo.get(0)).toString())
        .replace("$nispero$", nispero)
        .replace("@workerInstances", console.workerInstances(AWSTools.describeAutoScalingGroup(console.nisperon.aws, workersGroup)).toString())
      HtmlCustom(mainPage)
    }

    case GET(Path(Seg("record" :: id :: Nil))) => {
      ResponseString("id=" + id) ~> Ok
    }

    case GET(Path("/main.css")) => {
      val main = new FileInputStream(new File("src/main/resources/console/main.css"))
      val mainPage = io.Source.fromInputStream(main).mkString
      CssContent ~> ResponseString(mainPage)
    }
  }
}

case class HtmlCustom(s: String) extends ComposeResponse(HtmlContent ~> ResponseString(s))


object Server {
  val password = "password"

  object users extends Users {
    override def auth(u: String, p: String): Boolean = u.equals("nispero") && p.equals(password)
  }

  val meta = meta4.meta4
  val console = new Console(meta)

  val server = Https(443, "localhost")
    .handler(new ConsolePlan("metapasta", users, console))
  

  def shutdown() {
    Runtime.getRuntime().halt(0)
  }


  def main(args: Array[String]) {
    val console = new Console(meta4.meta4)
   // new File("keystore").delete()
    //import scala.sys.process._
   // val keyConf = io.Source.fromFile("keytoolConf.txt").mkString.replace("$password$", password)
    //println(keyConf)
   // val is = new ByteArrayInputStream(keyConf.getBytes("UTF-8"))
  //  ("keytool -keystore keystore -alias netty  -genkey -keyalg RSA -storepass $password$".replace("$password$", password) #< is).!

    println("finished")
    System.setProperty("netty.ssl.keyStore", "keystore")
    System.setProperty("netty.ssl.keyStorePassword", password)

    server.start()

  }

}
