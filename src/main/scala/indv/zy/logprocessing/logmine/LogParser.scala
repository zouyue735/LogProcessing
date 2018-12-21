package indv.zy.logprocessing.logmine

trait LogParser {
  def parse(log: String): Seq[Word]
}