package indv.zy.logprocessing.logmine

class Log(val words: Seq[Word])

sealed abstract class Word

case class Const(value: String) extends Word

case class Variable[A](value: A) extends Word

case class Wildcard() extends Word