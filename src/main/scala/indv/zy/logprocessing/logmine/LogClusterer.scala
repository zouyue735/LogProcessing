package indv.zy.logprocessing.logmine

trait LogClusterer {
  def cluster(clusters: Iterable[Log], log: Log): Option[Log]
}