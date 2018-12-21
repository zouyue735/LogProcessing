package indv.zy.logprocessing.logmine.impl

import indv.zy.logprocessing.logmine.{LogParser, Word}

object DefaultLogParser extends LogParser{
  override def parse(log: String): Seq[Word] = ???
}