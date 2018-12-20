package indv.zy.logprocessing.logmine.impl

import indv.zy.logprocessing.logmine.Log

object LogMineFramework {

  def main(args: Array[String]): Unit = {
  }
}

sealed abstract class PatternTree {
  val pattern: Log
}

case class Branch(pattern: Log, children: Seq[PatternTree]) extends PatternTree
case class Leaf(pattern: Log) extends PatternTree