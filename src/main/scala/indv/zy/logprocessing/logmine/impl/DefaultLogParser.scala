package indv.zy.logprocessing.logmine.impl

import indv.zy.logprocessing.logmine.{Const, Log, LogParser}

object DefaultLogParser extends LogParser {
  override def parse(log: String): Log = new Log(log.split("\\s").map(Const))
}