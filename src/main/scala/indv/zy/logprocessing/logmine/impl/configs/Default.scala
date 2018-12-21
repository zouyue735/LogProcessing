package indv.zy.logprocessing.logmine.impl.configs

import indv.zy.logprocessing.logmine.impl.{DefaultLogClusterer, DefaultLogParser, DefaultLogPatternGenerator, DefaultLogTokenizer}
import indv.zy.logprocessing.logmine.{LogClusterer, LogParser, LogPatternGenerator, LogTokenizer}

object Default {
  implicit val parser: LogParser = DefaultLogParser
  implicit val tokenizer: LogTokenizer = DefaultLogTokenizer
  implicit val clusterer: LogClusterer = DefaultLogClusterer
  implicit val patternGenerator: LogPatternGenerator = DefaultLogPatternGenerator
}