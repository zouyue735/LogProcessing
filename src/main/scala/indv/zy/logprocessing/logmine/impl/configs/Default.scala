package indv.zy.logprocessing.logmine.impl.configs

import indv.zy.logprocessing.logmine.{LogClusterer, LogPatternGenerator, LogTokenizer}
import indv.zy.logprocessing.logmine.impl.{DefaultLogClusterer, DefaultLogPatternGenerator, DefaultLogTokenizer}

object Default {
  implicit val tokenizer: LogTokenizer = DefaultLogTokenizer
  implicit val clusterer: LogClusterer = DefaultLogClusterer
  implicit val patternGenerator: LogPatternGenerator = DefaultLogPatternGenerator
}