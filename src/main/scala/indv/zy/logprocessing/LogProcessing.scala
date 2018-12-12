package indv.zy.logprocessing

import java.io.PrintWriter
import java.nio.charset.CodingErrorAction
import java.util

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.io.{Codec, Source}
import scala.util.Try

object LogProcessing {
  type Node = Function1[StreamSupplier, Map[String, Stream[_]]]

  val streams: mutable.Map[String, Stream[_]] = mutable.Map()
  val nodes: mutable.Buffer[Node] = mutable.Buffer()
  val ss = new StreamSupplier(() => streams)

  def start(): Unit = {
    val futures = for (n <- nodes) yield Future {
      val produced = n(ss)
      this.synchronized {
        produced.foreach({ case (k, v) => streams.put(k, v) })
      }
    }
    Await.result(Future.sequence(futures), scala.concurrent.duration.Duration.Inf)
  }

  def main(args: Array[String]): Unit = {
    val starts: mutable.Map[String, JsonLog] = mutable.Map()

    case class BillingApiCall(start: LocalDateTime, end: LocalDateTime, sessionId: String, billingId: String, methodName: String, processTime: Long, methodArguments: JsObject)

    streams.put("billing_api_logs",
      Source.fromFile("/home/zouyue/billingApiPerf/billing_all")(Codec.UTF8.onMalformedInput(CodingErrorAction.REPLACE).decodingReplaceWith("#")).getLines().toStream)
    nodes += (ss => {
      Map(("applicationLog",
        ss.getStream[String]("billing_api_logs").map(ApplicationLog.apply("otms-core-api", "unknown", "unknown", 0L, _))))
    })
    nodes += (ss => {
      Map(("jsonLog",
        ss.getStream[ApplicationLog]("applicationLog").map(log => Try(JsonLog.apply(log))).filter(_.isSuccess).map(_.get)))
    })
    nodes += (ss => {
      Map(("result", ss.getStream[JsonLog]("jsonLog").map(jsonLog => {
        val msg = jsonLog.json.getFields("msg").head.convertTo[String]
        if (msg == "WEB_SERVICE_LOG_ENTRY") {
          starts.put(jsonLog.log.thread, jsonLog)
          Option.empty
        } else if (msg == "WEB_SERVICE_LOG_END") {
          starts.remove(jsonLog.log.thread).map((_, Left(jsonLog)))
        } else {
          starts.remove(jsonLog.log.thread).map((_, Right(jsonLog)))
        }
      }).filter(_.isDefined).map(_.get)))
    })
    nodes += (ss => {
      Map(("convert", ss.getStream[Tuple2[JsonLog, Either[JsonLog, JsonLog]]]("result").map(result => {
        val start = result._1
        result._2 match {
          case Left(end) =>
            val arguments = start.json.getFields("methodArguments").head.asJsObject
            Some(BillingApiCall(start.log.timestamp, end.log.timestamp,
              if (arguments.getFields("sessionId").isEmpty) null else arguments.getFields("sessionId").head.convertTo[String],
              null,
              start.json.getFields("targetMethod").head.convertTo[String],
              end.json.getFields("processingTime").head.convertTo[Long],
              arguments))
          case Right(ex) => Option.empty
        }
      }).filter(_.isDefined).map(_.get).filter(_.sessionId != null)))
    })

    start()

    val apiLogs = ss.getStream[BillingApiCall]("convert")

    val stats = apiLogs.groupBy(_.methodName).mapValues(logs => {
      val avg = logs.foldLeft(0L)((a, l) => a + l.processTime) / logs.length
      val count = logs.length
      val max = logs.maxBy(_.processTime).processTime
      (count, avg, max)
    })
    println(stats.toJson)

    class Process(val logs: Seq[BillingApiCall]) {
      val signature: Seq[String] = logs.map(_.methodName)
      lazy val freePeriod: Long = {
        diff.map({
          case (l2, l1) =>
            Math.max(l2.start.toDate.getTime - l1.end.toDate.getTime, 0)
        }).sum
      }
      lazy val intersectPeriod: Long = {
        diff.map({
          case (l2, l1) =>
            Math.max(l1.end.toDate.getTime - l2.start.toDate.getTime, 0)
        }).sum
      }
      lazy val period: Long = {
        logs.last.end.toDate.getTime - logs.head.start.toDate.getTime
      }

      def diff: Seq[Tuple2[BillingApiCall, BillingApiCall]] = {
        logs.tail.zip(logs.dropRight(1))
      }

      override def toString: String = String.join(",", util.Arrays.asList(signature.toString, period.toString, freePeriod.toString, intersectPeriod.toString))
    }

    val processes: Map[String, Seq[BillingApiCall]] = apiLogs.groupBy(_.sessionId).mapValues(_.sortBy(_.start))
    nodes.clear()
    streams.clear()

    def getSequences[A](seq: Seq[A]): Seq[Seq[A]] = {
      def getSequencePair(seq: Seq[A]): Tuple2[mutable.Buffer[mutable.Buffer[A]], mutable.Buffer[mutable.Buffer[A]]] = {
        if (seq.length == 1) (mutable.Buffer(seq.toBuffer), mutable.Buffer.empty)
        else {
          val p = getSequencePair(seq.tail)
          p._2 ++= p._1
          p._1.indices.foreach(idx => p._1.update(idx, p._1(idx) += seq.head))
          p._1 += mutable.Buffer(seq.head)
          (p._1, p._2)
        }
      }

      val start = System.currentTimeMillis()
      val p = getSequencePair(seq)
      p._1 ++ p._2
    }


    def combine(t1: Tuple4[Long, Long, Long, Long], t2: Tuple4[Long, Long, Long, Long]): Tuple4[Long, Long, Long, Long] = {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4)
    }

    val writer = new PrintWriter("./out/sequences")
    processes.foreach({ case (session, seqs) =>
      println(seqs.size)
      getSequences(seqs)
        .map(new Process(_)).foreach(writer.println(_))
      writer.flush()
    })
  }

}

sealed class StreamSupplier(val streams: () => mutable.Map[String, Stream[_]]) {
  def getStream[A]: mutable.Map[String, Stream[A]] = {
    this.streams().map({ case (k, v) => (k, v.asInstanceOf[Stream[A]]) }).withDefault({
      Thread.sleep(50L)
      this.getStream(_)
    })
  }
}

case class ApplicationLog(service: String, machine: String, fileName: String, lineNumber: Long, timestamp: LocalDateTime, level: String, loggerName: String, thread: String, msg: String)

case class JsonLog(log: ApplicationLog, json: JsObject)

object ApplicationLog {

  lazy val localDateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss,SSS")

  def apply(service: String, machine: String, fileName: String, lineNumber: Long, line: String): ApplicationLog = {
    var idx1 = 0
    var idx2 = idx1 + 23
    val timestamp = localDateTimeFormat.parseLocalDateTime(line.substring(idx1, idx2))

    idx1 = idx2 + 1
    idx2 = line.indexOf(" ", idx1)
    val level = line.substring(idx1, idx2)

    val idx0 = idx2 + 1
    idx2 = line.indexOf(" - ", idx2)
    idx1 = line.lastIndexOf(" ", idx2 - 1) + 1
    val loggerName = line.substring(idx1, idx2)
    val msg = line.substring(idx2 + 3)
    val thread = line.substring(idx0, idx1 - 1)

    ApplicationLog(service, machine, fileName, lineNumber, timestamp, level, loggerName, thread, msg)
  }
}

object JsonLog {
  def apply(log: ApplicationLog): JsonLog = {
    new JsonLog(log, log.msg.parseJson.asJsObject)
  }
}