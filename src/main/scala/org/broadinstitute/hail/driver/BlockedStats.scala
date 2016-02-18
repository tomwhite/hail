package org.broadinstitute.hail.driver

import org.apache.spark.util.StatCounter
import org.broadinstitute.hail.Utils._
import scala.collection.mutable

/*
class Histogram extends Serializable {
  val a: Array[StatCounter] = Array.fill(BlockedStats.readLength)(new StatCounter())

  def merge(dstart: Int, d: Int): Histogram = {
    a(dstart).merge(d)
    this
  }

  def merge(h2: Histogram): Histogram = {
    for (i <- a.indices)
      a(i).merge(h2.a(i))
    this
  }

  def print() {
    for (i <- a.indices) {
      println(s"  $i: ${a(i)}")
    }
  }
}
*/

class Histogram extends Serializable {
  val m: mutable.Map[(Int, Int), Long] = mutable.Map.empty

  def merge(dstart: Int, d: Int): Histogram = {
    m.updateValue((dstart, d), 0, _ + 1)
    this
  }

  def merge(h2: Histogram): Histogram = {
    h2.m.foreach { case (k, v) =>
      m.updateValue(k, 0, _ + v)
    }
    this
  }

  def print() {
    for (i <- 0 until BlockedStats.readLength;
      j <- 0 until 100)
      println(s"$i\t$j\t${m.getOrElse((i, j), 0)}")
    /*
      m.foreach { case (k, v) =>
        println(s"  $k -> $v")
    */
  }
}

object BlockedStats extends Command {

  class Options extends BaseOptions

  def newOptions = new Options

  def name = "blockedstats"

  def description = "Print blocked stats in current dataset"

  override def supportsMultiallelic = true

  final val readLength = 125

  def gqBin(gq: Int): Int = {
    assert(gq <= 99)
    if (gq < 60)
      gq
    else
      (gq / 10) * 10
  }

  def run(state: State, options: Options): State = {
    val vds = state.vds

    // density
    val (variantDensitySC, nClose) = vds.rdd.map { case (v, va, gs) =>
      (v.contig, v.start)
    }.groupByKey()
      .mapValues { c =>
        val sorted = c.toArray.sorted
        val it = sorted.iterator
        val sc = new StatCounter()
        var nClose = 0
        if (it.hasNext) {
          var prev = it.next()
          while (it.hasNext) {
            val next = it.next()
            val d = next - prev
            if (d < readLength) {
              sc.merge(d)
              nClose += 1
            }
            prev = next
          }
        }
        (sc, nClose)
      }.map(_._2)
      .fold((new StatCounter(), 0)) { case ((sc1, nClose1), (sc2, nClose2)) => (sc1.merge(sc2), nClose1 + nClose2) }
    println(s"variantDensitySC = $variantDensitySC, nClose = $nClose")

    val (n, save, dpHist, gqHist) = vds.mapWithKeys { case (v, s, g) =>
      ((v.contig, s), (v.start, g.gt, g.dp, g.gq))
    }.groupByKey()
      .mapValues { c =>
        val sorted = c.toArray.sortWith { case ((s1, _, _, _), (s2, _, _, _)) => s1 < s2 }
        val it = sorted.iterator
        var n: Long = 0
        var save: Long = 0
        val dpHist = new Histogram()
        val gqHist = new Histogram()
        if (it.hasNext) {
          var prev = it.next()
          while (it.hasNext) {
            val next = it.next()
            val dstart = next._1 - prev._1

            if (dstart < readLength) {
              n += 1
              if (prev._2 == next._2
                && prev._4.map(gqBin) == next._4.map(gqBin))
                save += 1

              prev._3.map { dpx1 =>
                next._3.map { dpx2 =>
                  val ddp = (dpx2 - dpx1).abs.min(99)
                  dpHist.merge(dstart, ddp)
                }
              }

              prev._4.map { gqx1 =>
                next._4.map { gqx2 =>
                  val dgq = (gqx2 - gqx1).abs.min(99)
                  gqHist.merge(dstart, dgq)
                }
              }
            }

            prev = next
          }
        }
        (n, save, dpHist, gqHist)
      }.map(_._2)
      .fold((0L, 0L, new Histogram(), new Histogram())) {
        case ((n1, save1, dpHist1, gqHist1), (n2, save2, dpHist2, gqHist2)) =>
          (n1 + n2, save1 + save2, dpHist1.merge(dpHist2), gqHist1.merge(gqHist2))
      }
    println(s"n = $n, save = $save")
    println("dpHist:")
    dpHist.print()

    println("gqHist:")
    gqHist.print()

    state
  }
}
