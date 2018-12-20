package indv.zy.logprocessing

import java.io.{File, PrintWriter}
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
    val threadStarts: mutable.Map[String, JsonLog] = mutable.Map()
    val sessionStarts: mutable.Map[String, BillingApiCall] = mutable.Map()

    streams.put("logs_files", new File("/home/zouyue/billingApiPerf/billing_api_perf").listFiles().sortBy(_.getName).toStream)
    nodes += (ss => {
      Map(("billing_api_logs",
        ss.getStream[File]("logs_files").flatMap(Source.fromFile(_)(Codec.UTF8.onMalformedInput(CodingErrorAction.REPLACE).decodingReplaceWith("#")).getLines().toStream)))
    })
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
          threadStarts.put(jsonLog.log.thread, jsonLog)
          Option.empty
        } else if (msg == "WEB_SERVICE_LOG_END") {
          threadStarts.remove(jsonLog.log.thread).map((_, Left(jsonLog)))
        } else {
          threadStarts.remove(jsonLog.log.thread).map((_, Right(jsonLog)))
        }
      }).filter(_.isDefined).map(_.get)))
    })
    nodes += (ss => {
      Map(("convert", ss.getStream[Tuple2[JsonLog, Either[JsonLog, JsonLog]]]("result").map(result => {
        val start = result._1
        result._2 match {
          case Left(end) =>
            val arguments = start.json.getFields("methodArguments").head.asJsObject
            val billingId = Try(start.json.getFields("body").head.asJsObject.getFields("billingId").head.convertTo[String])
              .orElse(Try(arguments.getFields("billingId").head.convertTo[String]))
            Some(BillingApiCall(start.log.timestamp, end.log.timestamp,
              if (arguments.getFields("sessionId").isEmpty) null else arguments.getFields("sessionId").head.convertTo[String],
              billingId.getOrElse(null),
              start.json.getFields("targetMethod").head.convertTo[String],
              end.json.getFields("processingTime").head.convertTo[Long],
              arguments))
          case Right(ex) => Option.empty
        }
      }).filter(_.isDefined).map(_.get).filter(_.sessionId != null)))
    })
    nodes += (ss => {
      Map(("openBilling", ss.getStream[BillingApiCall]("convert").map(call => {
        if (call.methodName == "viewBilling" && call.billingId != null) {
          sessionStarts.put(call.sessionId, call)
          Option.empty
        } else if (call.methodName == "getOrderList" && call.billingId != null) {
          sessionStarts.remove(call.sessionId).filter(_.billingId == call.billingId).map((_, call))
        } else {
          Option.empty
        }
      }).filter(_.isDefined).map(_.get)))
    })

    start()

    val bins = Seq((0, 100), (100, 1000), (1000, 10000), (10000, 100000), (100000, Integer.MAX_VALUE))
    val counts: mutable.TreeMap[String, mutable.Map[(String, Boolean), mutable.TreeMap[(Int, Int), (Int, Long)]]] = mutable.TreeMap()
    counts.put("s136", mutable.TreeMap())
    counts.put("s137", mutable.TreeMap())
    counts.put("s138", mutable.TreeMap())

    val bacss: Seq[BillingApiCallStat] = (for {
      l <- ss.getStream[Tuple2[BillingApiCall, BillingApiCall]]("openBilling").toList ++ sessionStarts.values.map((_, null))
    } yield {
      val localDateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      val p = if (l._2 == null) {
        new Process(Seq(l._1))
      } else new Process(Seq(l._1, l._2))
      val decryptedId = Decrypts.decrypts.getOrElse(l._1.billingId, 0)
      val data = Database.billings.getOrElse(decryptedId, (null, null, null, null, null, null, null))
      BillingApiCallStat(p.period, p.freePeriod, p.intersectPeriod, p.period - p.freePeriod, p.period + p.intersectPeriod, p.start, decryptedId,
        data._1 match {
          case 10 => "OTMS"
          case 20 => "BSEC"
          case 30 => "FAP"
          case _ => null
        }, data._2.asInstanceOf[Long],
        data._3 match {
          case 10 => "CLIENT"
          case 20 => "OFFLINE_VENDOR"
          case 30 => "VENDOR"
          case 40 => "OFFLINE_CLIENT"
          case _ => null
        }, data._4.asInstanceOf[Long], data._5.asInstanceOf[Int], data._6.asInstanceOf[Boolean], if (data._7 != null) LocalDateTime.parse(data._7, localDateTimeFormat) else null, p.logs.length == 1)
    }).filter(_.billingSource != null).filter(_.billingSize > 0).filter(bacs => bacs.generateDate == null || bacs.generateDate.isBefore(bacs.date)) //.filter(_.serverPeriod < 600000)

    bacss.groupBy(s => (s.billingSource, s.isShipper)).foreach({
      case ((source, isShipper), seq) =>
        val writer = new PrintWriter("./out/" + source + "_" + isShipper + ".csv")
        //        case class BillingApiCallStat(period: Long, freePeriod: Long, intersectPeriod: Long, waitPeriod: Long, serverPeriod: Long, month: Int, day: Int, billingId: Long, billingSource: String, owner: Long, party: String, partner: Long, billingSize: Int, isShipper: Boolean) {
        writer.println("period,free_period,intersect_period,wait_period,server_period,date,billing_id,billing_source,owner,billing_party,partner,billing_size,is_shipper,sprint,partial")
        seq.sortBy(_.date).foreach(bacs => writer.println(bacs.toCsvLine))
        writer.flush()
        writer.close()

        counts("s136").put((source, isShipper), mutable.TreeMap())
        counts("s137").put((source, isShipper), mutable.TreeMap())
        counts("s138").put((source, isShipper), mutable.TreeMap())
        seq.foreach(bacs => {
          val idx = bins.indexWhere({ case (min, max) => bacs.billingSize >= min && bacs.billingSize < max })
          val range = bins(idx)
          val map = counts(sprint(bacs))((source, isShipper))
          map.put(range, (map.getOrElse(range, (0, 0L))._1 + 1, map.getOrElse(range, (0, 0L))._2 + bacs.serverPeriod))
        })
    })

    def sprint(bacs: BillingApiCallStat): String = {
      if (bacs.date.isAfter(new LocalDateTime(2018, 12, 10, 4, 0)))
        "s138"
      else if (bacs.date.isAfter(new LocalDateTime(2018, 11, 19, 4, 0)))
        "s137"
      else "s136"
    }

    (for {
      (sprint, m1) <- counts
      ((source, isShipper), m2) <- m1
      ((min, max), (count, time)) <- m2
    } yield {
      (sprint, source, isShipper, min + "-" + max, time.doubleValue() / count.doubleValue())
    }).toList.sortBy(t => (t._2, t._3, t._4, t._1)).foreach(t => println(t._2, t._3, t._4, t._1, t._5))

    System.exit(1)

    val apiLogs = ss.getStream[BillingApiCall]("convert")

    val stats = apiLogs.groupBy(_.methodName).mapValues(logs => {
      val avg = logs.foldLeft(0L)((a, l) => a + l.processTime) / logs.length
      val count = logs.length
      val max = logs.maxBy(_.processTime).processTime
      (count, avg, max)
    })
    println(stats.toJson)

    val processes: Map[String, Seq[BillingApiCall]] = apiLogs.groupBy(_.sessionId).mapValues(_.sortBy(_.start))
    nodes.clear()
    streams.clear()

    //    def getSequences[A](seq: Seq[A]): Seq[Seq[A]] = {
    //      def getSequencePair(seq: Seq[A]): Tuple2[mutable.Buffer[mutable.Buffer[A]], mutable.Buffer[mutable.Buffer[A]]] = {
    //        if (seq.length == 1) (mutable.Buffer(seq.toBuffer), mutable.Buffer.empty)
    //        else {
    //          val p = getSequencePair(seq.tail)
    //          p._2 ++= p._1
    //          p._1.indices.foreach(idx => p._1.update(idx, p._1(idx) += seq.head))
    //          p._1 += mutable.Buffer(seq.head)
    //          (p._1, p._2)
    //        }
    //      }
    //
    //      val start = System.currentTimeMillis()
    //      val p = getSequencePair(seq)
    //      p._1 ++ p._2
    //    }

    def printSequences(session: String, seq: Seq[BillingApiCall], writer: PrintWriter): Unit = {
      def printInternal(remaining: Seq[BillingApiCall], prefixes: Seq[Seq[BillingApiCall]]): Unit = {
        if (remaining.nonEmpty) {
          val newPrefixes = prefixes.map(p => p.:+(remaining.head)).:+(remaining.take(1))
          newPrefixes.foreach(print)

          printInternal(remaining.tail, newPrefixes)
        }
      }

      def print(logs: Seq[BillingApiCall]): Unit = {
        val p = new Process(logs)
        writer.println("Insert into process (id, signature, period, free_period, intersect_period, start_timestamp, end_timestamp, step_count, session) VALUES " +
          f"(nextval(\'seq_process\'), \'${p.signature.toJson}\', ${p.period}, ${p.freePeriod}, ${p.intersectPeriod}, \'${p.start.toString("yyyy-MM-dd HH:mm:ss.SSS")}\', \'${p.end.toString("yyyy-MM-dd HH:mm:ss.SSS")}\', ${p.stepCount}, \'$session\');")
      }

      printInternal(seq, Seq())
    }

    val writer = new PrintWriter("./out/sequences")
    val total = processes.mapValues(_.size).values.map(s => (s * s + s) / 2).sum
    var current = 0
    processes.foreach({ case (session, seqs) =>
      println(seqs.size + " start at " + LocalDateTime.now())
      printSequences(session, seqs, writer)
      writer.flush()
      current = current + (seqs.size * seqs.size + seqs.size) / 2
      println(current + "/" + total + " at " + LocalDateTime.now())
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

case class BillingApiCall(start: LocalDateTime, end: LocalDateTime, sessionId: String, billingId: String, methodName: String, processTime: Long, methodArguments: JsObject)

case class BillingApiCallStat(period: Long, freePeriod: Long, intersectPeriod: Long, waitPeriod: Long, serverPeriod: Long, date: LocalDateTime, billingId: Long, billingSource: String, owner: Long, party: String, partner: Long, billingSize: Int, isShipper: Boolean, generateDate: LocalDateTime, partial: Boolean) {
  def toCsvLine: String = {
    String.join(",", period.toString, freePeriod.toString, intersectPeriod.toString, waitPeriod.toString, serverPeriod.toString, date.toString, billingId.toString, billingSource, owner.toString, party, partner.toString, billingSize.toString, isShipper.toString,
      if (date.isAfter(new LocalDateTime(2018, 12, 10, 4, 0)))
        "s138"
      else if (date.isAfter(new LocalDateTime(2018, 11, 19, 4, 0)))
        "s137"
      else "s136", partial.toString)
  }
}

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
  lazy val start: LocalDateTime = logs.head.start
  lazy val end: LocalDateTime = logs.last.end
  lazy val stepCount: Int = logs.size

  def diff: Seq[Tuple2[BillingApiCall, BillingApiCall]] = {
    logs.tail.zip(logs.dropRight(1))
  }

  override def toString: String = String.join(",", util.Arrays.asList(signature.toString, period.toString, freePeriod.toString, intersectPeriod.toString))
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