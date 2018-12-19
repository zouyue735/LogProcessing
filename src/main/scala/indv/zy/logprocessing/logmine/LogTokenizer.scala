package indv.zy.logprocessing.logmine

trait LogTokenizer {
  def tokenize(log: Log): Log
}