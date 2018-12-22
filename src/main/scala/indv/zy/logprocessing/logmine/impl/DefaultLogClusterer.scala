package indv.zy.logprocessing.logmine.impl

import indv.zy.logprocessing.logmine.{Log, LogClusterer}

object DefaultLogClusterer extends LogClusterer {
  override def cluster(clusters: Iterable[Log], log: Log): Option[Log] = ???
}
