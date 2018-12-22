package indv.zy.logprocessing.logmine

class LogMineFramework(implicit val parser: LogParser, val tokenizer: LogTokenizer, val clusterer: LogClusterer, val patternGenerator: LogPatternGenerator) {

  def logMine(logs: Iterable[RawLog]): PatternTree = {
    def iteration(patterns: Iterable[PatternTree]): Iterable[PatternTree] = {
      if (patterns.size == 1) {
        patterns
      } else {
        val clusters = patterns.foldLeft(Map[Log, List[PatternTree]]())((clusters, tree) => {
          clusterer.cluster(clusters.keys, tree.pattern) match {
            case Some(cluster) => {
              // TODO cluster event
              clusters.updated(cluster, tree :: clusters(cluster))
            }
            case None => {
              // TODO cluster event
              clusters.updated(tree.pattern, List(tree))
            }
          }
        })

        clusters.mapValues(cp => {
          // TODO pattern event
          val pattern = patternGenerator.generatePattern(cp.map(_.pattern))
          Branch(pattern, cp)
        }).values
      }
    }

    iteration(logs.map(l => {
      val parsed = parser.parse(l.log())
      // TODO parse event
      parsed
    }).map(l => {
      val tokenized = tokenizer.tokenize(l)
      // TODO tokenize event
      tokenized
    }).foldLeft(List[Log]())((clusters, log) => {
      clusterer.cluster(clusters, log) match {
        case Some(cluster) => {
          // TODO cluster event
          // TODO pattern event
          clusters
        }
        case None => {
          // TODO cluster event
          // TODO pattern event
          log :: clusters
        }
      }
    }).map(Leaf)).head
  }

  def main(args: Array[String]): Unit = {
  }
}

sealed abstract class PatternTree {
  val pattern: Log
}

case class Branch(pattern: Log, children: Seq[PatternTree]) extends PatternTree

case class Leaf(pattern: Log) extends PatternTree

trait ParseListener {

}

trait TokenizeListener {

}

trait ClusterListener {

}

trait PatternListener {

}