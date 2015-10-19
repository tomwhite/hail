package org.broadinstitute.k3.methods

import org.apache.spark.util.StatCounter
import org.broadinstitute.k3.variant._
import org.broadinstitute.k3.Utils._

import scala.collection.mutable

object dpStatCounterPer extends AggregateMethod {
  def name = "dpMean\tdpStDev"

  type T = StatCounter

  override def aggZeroValue = StatCounter()

  override def seqOpWithKeys(v: Variant, s: Int, g: Genotype, sc: StatCounter): StatCounter = {
    if (g.isCalled)
      sc.merge(g.dp)
    sc
  }

  override def combOp(sc1: StatCounter, sc2: StatCounter) = sc1.merge(sc2)

  override def emit(sc: T, b: mutable.ArrayBuilder[Any]) {
    b += sc.mean
    b += sc.stdev
  }
}


object dpStatCounterPerGenotype extends AggregateMethod {
  def name = "gqMeanHomRef\tgqStDevHomRef\tgqMeanHet\tgqStDevHet\tgqMeanHomVar\tgqStDevHomVar"

  type T = (StatCounter, StatCounter, StatCounter)

  override def aggZeroValue: T = (StatCounter(), StatCounter(), StatCounter())

  override def seqOpWithKeys(v: Variant, s: Int, g: Genotype,
                             scs: T): T = {
    if (g.isCalled) {
      if (g.isHomRef)
        scs._1.merge(g.gq)
      if (g.isHet)
        scs._2.merge(g.gq)
      if (g.isHomVar)
        scs._3.merge(g.gq)
    }
    scs
  }

  override def combOp(scs1: T, scs2: T): T = (scs1._1.merge(scs2._1), scs1._2.merge(scs2._2), scs1._3.merge(scs2._3))

  override def emit(scs: T, b: mutable.ArrayBuilder[Any]) {
    b += scs._1.mean
    b += scs._1.stdev
    b += scs._2.mean
    b += scs._2.stdev
    b += scs._3.mean
    b += scs._3.stdev
  }
}