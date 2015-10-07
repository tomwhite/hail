package org.broadinstitute.k3.methods

import org.broadinstitute.k3.stats.LeveneHaldane

object HWEPerVariant extends DerivedMethod {
  type T = (Option[Double], Double)

  def name = "rExpectedHetFrequency\tpHWE"

  def map(values: MethodValues) = {

    val n = values.get(nCalledPer)
    val nAB = values.get(nHetPer)
    val nA = nAB + 2 * math.min(values.get(nHomRefPer), values.get(nHomVarPer))

    val LH = LeveneHaldane(n, nA)

    (if (n != 0) Some(LH.getNumericalMean / n) else None, LH.exactMidP(nAB))
  }
}