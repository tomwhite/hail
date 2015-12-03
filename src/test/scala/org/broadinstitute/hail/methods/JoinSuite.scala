package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test
import org.broadinstitute.hail.variant.VariantSampleMatrix

class JoinSuite extends SparkSuite {
  @Test def test() {
    import VariantSampleMatrix._
    val gt1 = Array(
      Array(0, 1, 2, 1, 0),
      Array(-1, 1, 0, 1, 0),
      Array(2, 1, 1, 1, 2),
      Array(2, -1, 1, -1, 0),
      Array(0, 1, 2, 2, -1))

    val gt2 = Array(
      Array(2, 0, 2, 1, 0),
      Array(2, -1, 0, -1, 0),
      Array(0, 2, 1, -1, 2),
      Array(-1, -1, -1, 0, -1),
      Array(1, 1, 1, 1, 1))

    val vsm1 = TestRDDBuilder.buildRDD(5, 5, sc, gtArray = Some(gt1),sampleIds=Some(Array("s1", "s2", "s3", "s4", "s5")))
    val vsm2 = TestRDDBuilder.buildRDD(5, 5, sc, gtArray = Some(gt2),sampleIds=Some(Array("foo", "s7", "beet", "pug", "s3")))

    val foj = vsm1.fullOuterJoin(vsm2)
    val loj = vsm1.leftOuterJoin(vsm2)
    val roj = vsm1.rightOuterJoin(vsm2)
    val ij = vsm1.innerJoin(vsm2).takeSample(false,5,0)

  }
}
