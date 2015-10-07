package org.broadinstitute.k3.methods

import org.broadinstitute.k3.SparkSuite
import org.broadinstitute.k3.driver.VariantQC
import org.broadinstitute.k3.stats.LeveneHaldane
import org.testng.annotations.Test
import org.broadinstitute.k3.Utils._

class HWESuite extends SparkSuite {

  @Test def test() {
    val vds = LoadVCF(sc, "src/test/resources/HWE_test.vcf")

    val methods: Array[AggregateMethod] = Array(
      nCalledPer, nHomRefPer, nHetPer, nHomVarPer
    )

    val derivedMethods: Array[DerivedMethod] = Array(
      rHetFrequencyPer, HWEPerVariant
    )

    val r = VariantQC.results(vds, methods, derivedMethods).collect().map{case (v, a) => (v.start, a)}.toMap

    //for (i <- r.keys) {print("\n" + i); for (j <- r(i)) print("\t" + j)}

    assert(r(1)(5) == (Some(0.0), 0.5))
    assert(r(2)(5) == (Some(0.25), 0.5))
    assert(compareDouble(r(3)(5).asInstanceOf[(Option[Double], Double)]._2, LeveneHaldane(4, 3).exactMidP(1)))
    assert(compareDouble(r(4)(5).asInstanceOf[(Option[Double], Double)]._2, LeveneHaldane(4, 4).exactMidP(2)))
    assert(compareDouble(r(5)(5).asInstanceOf[(Option[Double], Double)]._2, LeveneHaldane(3, 1).exactMidP(1)))
    assert(r(6)(5) == (None, 0.5))
  }
}