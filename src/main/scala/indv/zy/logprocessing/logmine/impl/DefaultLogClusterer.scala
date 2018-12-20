package indv.zy.logprocessing.logmine.impl

import indv.zy.logprocessing.logmine.{Log, LogClusterer}

object DefaultLogClusterer extends LogClusterer {
  override def initialCluster(logs: Seq[Log]): Seq[Log] = ???

  override def patternCluster(patterns: Seq[Log]): Seq[Seq[Log]] = ???
}
