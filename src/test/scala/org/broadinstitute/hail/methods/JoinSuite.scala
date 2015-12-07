package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test

class JoinSuite extends SparkSuite {
  @Test def test() {

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
    val vsm2 = TestRDDBuilder.buildRDD(5, 3, sc, gtArray = Some(gt2),sampleIds=Some(Array("foo", "s7", "beet", "pug", "s3")))

    val (vsm1PrimeInner,vsm2PrimeInner) = vsm1.reindexSamplesInnerJoin(vsm2)
    val (vsm1PrimeOuter,vsm2PrimeOuter) = vsm1.reindexSamplesOuterJoin(vsm2)
    val (vsm1PrimeLeft,vsm2PrimeLeft) = vsm1.reindexSamplesLeftJoin(vsm2)
    val (vsm1PrimeRight,vsm2PrimeRight) = vsm1.reindexSamplesRightJoin(vsm2)

    val x = Array((vsm1PrimeInner,vsm2PrimeInner),
      (vsm1PrimeOuter,vsm2PrimeOuter),
      (vsm1PrimeLeft,vsm2PrimeLeft),
      (vsm1PrimeRight,vsm2PrimeRight))

    for ((a,b) <- x) {
      val foj = a.fullOuterJoin(b)
      val loj = a.leftOuterJoin(b)
      val roj = a.rightOuterJoin(b)
      val ij = a.innerJoin(b)
    }
  }
}
