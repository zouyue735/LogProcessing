package indv.zy.logprocessing

import java.io.File

import scala.io.Source
import scala.util.Try

object Database {
  val dir = new File("/home/zouyue/billingApiPerf/")
  val billings: Map[Long, (Int, Long, Int, Long, Int, Boolean, String)] = (1 to 4).flatMap(n => {
    val base = Source.fromFile(new File(dir, f"billing_$n.csv")).getLines()
    val otmsOrder = Source.fromFile(new File(dir, f"billing_count_$n.csv")).getLines()
    val ecOrder = Source.fromFile(new File(dir, f"billing_count_ec_$n.csv")).getLines()
    base.zip(otmsOrder).zip(ecOrder).drop(1).map({
      case ((baseLine, otmsLine), ecLine) => {
        val baseParts = baseLine.split(",")
        val otmsParts = otmsLine.split(",")
        val ecParts = ecLine.split(",")

        val billingId = baseParts(0).toLong
        val billingSource = baseParts(1).toInt
        val owner = baseParts(2).toLong
        val billingParty = baseParts(3).toInt
        val partner = baseParts(4).toLong
        val isShipper = baseParts(5) match {
          case "f" => false
          case "t" => true
        }

        require(billingId == otmsParts(0).toLong)
        val otmsCount = otmsParts(1).toInt
        val otmsTime = Try(otmsParts(2)).getOrElse(null)

        require(billingId == ecParts(0).toLong)
        val ecCount = ecParts(1).toInt
        val ecTime = Try(ecParts(2)).getOrElse(null)

        val count = Math.max(otmsCount, ecCount)
        val time = if (otmsTime == null) ecTime else otmsTime

        (billingId, (billingSource, owner, billingParty, partner, count, isShipper, time))
      }
    })
  }).toMap
}
