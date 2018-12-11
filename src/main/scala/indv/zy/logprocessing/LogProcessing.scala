package indv.zy.logprocessing

import com.github.nscala_time.time.Imports._
import org.joda.time.format.DateTimeFormatter
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.io.Source
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
        produced.foreach({ case (k, v) => streams.put(k, v.map(Try(_)).filter(_.isSuccess).map(_.get)) })
      }
    }
    Await.result(Future.sequence(futures), scala.concurrent.duration.Duration.Inf)
  }

  def main(args: Array[String]): Unit = {
    val parseNode: Node = ss => {
      Map(("applicationLog",
        ss.getStream[String]("billing_api_logs").map(ApplicationLog.apply("otms-core-api", "unknown", "unknown", 0L, _))))
    }
    val jsonNode: Node = ss => {
      Map(("jsonLog",
        ss.getStream[ApplicationLog]("applicationLog").map(x => Try(JsonLog.apply(x))).filter(_.isSuccess).map(_.get)))
    }

    val starts: mutable.Map[String, JsonLog] = mutable.Map()
    val resultNode: Node = ss => {
      Map(("result", ss.getStream[JsonLog]("jsonLog").map(jsonLog => {
        if (jsonLog.json.getFields("msg").head.convertTo[String] == "WEB_SERVICE_LOG_ENTRY") {
          starts.put(jsonLog.log.thread, jsonLog)
          Option.empty
        } else {
          starts.remove(jsonLog.log.thread).map((_, jsonLog))
        }
      }).filter(_.isDefined).map(_.get)))
    }

    streams.put("billing_api_logs", Source.fromFile("/home/zouyue/billingApiPerf/billing_all").getLines().toStream)
    nodes += parseNode
    nodes += jsonNode
    nodes += resultNode

    start()

    ss.getStream[Tuple2[_, _]]("result").foreach(println)
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
    println(line)

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