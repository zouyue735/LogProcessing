package indv.zy.logprocessing.logmine

trait LogPatternGenerator {
   def generatePattern(logs: Iterable[Log]): Log
}