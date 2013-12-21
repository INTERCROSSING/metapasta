import ohnosequences.sbt._
import sbtrelease._
import ReleaseStateTransformations._
import ReleasePlugin._
import ReleaseKeys._
import AssemblyKeys._

publishMavenStyle := false

isPrivate := true

releaseSettings

releaseProcess <<= thisProjectRef apply { ref =>
  Seq[ReleaseStep](
    inquireVersions,
    setReleaseVersion,
    setNextVersion,
    publishArtifacts
  )
}

nextVersion := { ver => sbtrelease.Version(ver).map(_.bumpMinor.string).getOrElse(versionFormatError) }

addCommandAlias("metapasta-publish", ";reload; release with-defaults")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case "about.html" => MergeStrategy.first
  case "avsl.conf" => MergeStrategy.first
 // case PathList(_*) => MergeStrategy.first
 case PathList("META-INF", _*) => MergeStrategy.first
  case x => old(x)
}
}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {_.data.getName == "bio4j-scala-distribution_2.10-0.1.0-SNAPSHOT-fat.jar"}
}