package indv.zy.logprocessing.logmine

trait LogClusterer {
  def initialCluster(logs: Seq[Log]): Seq[Log]

  def patternCluster(patterns: Seq[Log]): Seq[Seq[Log]]
}