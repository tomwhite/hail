package org.broadinstitute.k3.methods

import org.broadinstitute.k3.stats.LeveneHaldane

import scala.collection.mutable

object HWEPerVariant extends DerivedMethod {
  type T = (Option[Double], Double)

  def name = "rExpectedHetFrequency\tpHWE"

  override def emit(values: MethodValues, b: mutable.ArrayBuilder[Any]) {
    val n = values.get(nCalledPer)
    val nAB = values.get(nHetPer)
    val nA = nAB + 2 * math.min(values.get(nHomRefPer), values.get(nHomVarPer))

    val LH = LeveneHaldane(n, nA)

    b += (if (n != 0) Some(LH.getNumericalMean / n) else None)
    b += LH.exactMidP(nAB)
  }
}
