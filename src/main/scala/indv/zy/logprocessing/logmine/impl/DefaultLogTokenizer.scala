package indv.zy.logprocessing.logmine.impl

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import indv.zy.logprocessing.logmine._

import scala.util.Try

object DefaultLogTokenizer extends LogTokenizer {

  val tokenizers: Seq[String => _] = Seq(
    {BigInt(_)},
    {BigDecimal(_)},
    {LocalDateTime.parse(_, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))},
  )

  override def tokenize(log: Log): Log = new Log(
    log.words.map({
      case Const(value) =>
        (for {
          tokenizer <- tokenizers
          variable <- Try(tokenizer(value)).toOption
        } yield Variable(variable)).headOption.getOrElse(Const(value))
      case Variable(value) => Variable(value)
      case Wildcard() => Wildcard()
    }))
}