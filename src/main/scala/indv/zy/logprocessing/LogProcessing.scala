package indv.zy.logprocessing

object LogProcessing {
  type Node = Function1[Function1[String, Stream[_]], Function1[String, Stream[_]]]

  def main(args: Array[String]): Unit = {
  }
}
