package ohnosequences.metapasta.reporting


trait AnyGroup {
  def name: String
  val samples: List[SampleId]
}

case class SamplesGroup(name: String, samples: List[SampleId]) extends AnyGroup

case class ProjectGroup(samples: List[SampleId]) extends AnyGroup {
  override def name: String = "project"
}
