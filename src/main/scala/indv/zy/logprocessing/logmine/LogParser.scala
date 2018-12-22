package indv.zy.logprocessing.logmine

trait LogParser {
  def parse(log: String): Log
}