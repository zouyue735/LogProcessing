package indv.zy.logprocessing.logmine

trait LogClusterer {
  def cluster(clusters: Seq[Log], log: Log): Option[Log]
}