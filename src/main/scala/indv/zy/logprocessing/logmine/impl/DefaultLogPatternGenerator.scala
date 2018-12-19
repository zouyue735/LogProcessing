package indv.zy.logprocessing.logmine.impl

import indv.zy.logprocessing.logmine.{Log, LogPatternGenerator}

object DefaultLogPatternGenerator extends LogPatternGenerator {
  override def generatePattern(logs: Seq[Log]): Log = ???
}