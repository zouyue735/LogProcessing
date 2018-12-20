package indv.zy.logprocessing.logmine

trait LogPatternGenerator {
   def generatePattern(logs: Seq[Log]): Log
}