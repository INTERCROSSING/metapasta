import sbt._
import Keys._
import com.amazonaws.auth._
import ohnosequences.sbt.nice.ResolverSettings._
import sbtassembly._
import AssemblyKeys._

object MetapastaBuild extends Build {

  val testCredentialsProvider = SettingKey[Option[AWSCredentialsProvider]]("credentials provider for test environment")

 // val testMainClass = SettingKey[Option[String]]("compota main class")

  override lazy val settings = super.settings ++ Seq(testCredentialsProvider := None)

  def providerConstructorPrinter(provider: AWSCredentialsProvider) = provider match {
    case ip: InstanceProfileCredentialsProvider => {
      "new com.amazonaws.auth.InstanceProfileCredentialsProvider()"
    }
    case ep: EnvironmentVariableCredentialsProvider => {
      "new com.amazonaws.auth.EnvironmentVariableCredentialsProvider()"
    }
    case pp: PropertiesFileCredentialsProvider => {
      val field = pp.getClass().getDeclaredField("credentialsFilePath")
      field.setAccessible(true)
      val path = field.get(pp).toString
      "new com.amazonaws.auth.PropertiesFileCredentialsProvider(\"\"\"$path$\"\"\")".replace("$path$", path)
    }

    //todo fix!
    case p => ""
  }

  def optionStringPrinter(o: Option[String]): String = o match {
    case None => "None"
    case Some(s) => "Some(\"" + s + "\")"
  }



  lazy val root = Project(id = "metapasta",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      sourceGenerators in Test += task[Seq[File]] {
        val text = """
                     |package ohnosequences.compota.test.generated
                     |
                     |object credentials {
                     |  val credentialsProvider: Option[com.amazonaws.auth.AWSCredentialsProvider] = $cred$
                     |}
                     |""".stripMargin
          .replace("$cred$", testCredentialsProvider.value.map(providerConstructorPrinter).toString)
        val file = (sourceManaged in Compile).value / "testCredentials.scala"
        IO.write(file, text)
        Seq(file)
      }
    )
  )
}