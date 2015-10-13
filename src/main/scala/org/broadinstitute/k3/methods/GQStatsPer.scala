package org.broadinstitute.k3.methods

import org.apache.spark.util.StatCounter
import org.broadinstitute.k3.variant._
import org.broadinstitute.k3.Utils._

import scala.collection.mutable

object gqStatCounterPer extends AggregateMethod {

  def name = "gqMean\tgqStDev"

  type T = StatCounter

  override def aggZeroValue = StatCounter()

  override def seqOpWithKeys(v: Variant, s: Int, g: Genotype, sc: StatCounter): StatCounter = {
    if (g.isCalled)
      sc.merge(g.gq)
    sc
  }

  override def combOp(sc1: StatCounter, sc2: StatCounter) = sc1.merge(sc2)
  override def emit(sc: StatCounter, b: mutable.ArrayBuilder[Any]) {
    b += sc.mean
    b += sc.stdev
  }
}