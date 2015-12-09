package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test

class JoinSuite extends SparkSuite {
  @Test def test() {

    val vsm1 = TestRDDBuilder.buildRDD(5, 3, sc,sampleIds=Some(Array("s1", "s2", "s3", "s4", "s5")))
    val vsm2 = TestRDDBuilder.buildRDD(5, 5, sc,sampleIds=Some(Array("foo", "s7", "beet", "pug", "s3")))
    vsm1.cache()
    vsm2.cache()

    val expectedNumSamples = Map("inner" -> 1, "left" -> 5, "right" -> 5, "outer" -> 9)
    val expectedNumVariants = Map("inner" -> 3, "left" -> 3, "right" -> 5, "outer" -> 5)

    val joinTypes = Array("inner","left","right","outer")

    for (sjt <- joinTypes; vjt <- joinTypes) {
      println(s"$sjt\t$vjt")
      val nSamples = expectedNumSamples(sjt)
      val nVariants = expectedNumVariants(vjt)

      val (vsm1Prime,vsm2Prime) = sjt match {
        case "inner" => vsm1.reindexSamplesInnerJoin(vsm2)
        case "left" => vsm1.reindexSamplesLeftJoin(vsm2)
        case "right" => vsm1.reindexSamplesRightJoin(vsm2)
        case "outer" => vsm1.reindexSamplesOuterJoin(vsm2)
        case _ => throw new UnsupportedOperationException
      }
      vsm1Prime.cache()
      vsm2Prime.cache()

      val mergedVSM = vjt match {
        case "inner" => vsm1Prime.innerJoin(vsm2Prime)
        case "left" => vsm1Prime.leftOuterJoin(vsm2Prime)
        case "right" => vsm1Prime.rightOuterJoin(vsm2Prime)
        case "outer" => vsm1Prime.fullOuterJoin(vsm2Prime)
        case _ => throw new UnsupportedOperationException
      }
      mergedVSM.cache()

      assert(mergedVSM.localSamples.length == nSamples)
      assert(mergedVSM.rdd.filter{case (v,g) => g.size == nSamples}.count == nVariants)

      val vsm1SampleIdsLocal = vsm1.sampleIds
      val vsm1PrimeSampleIdsLocal = vsm1Prime.sampleIds
      val vsm2SampleIdsLocal = vsm2.sampleIds
      val vsm2PrimeSampleIdsLocal = vsm2Prime.sampleIds
      val mergeSampleIdsLocal = mergedVSM.sampleIds

      val vsm1ExpandedMap = vsm1.expand().map{case (v,s,g) => ((v,vsm1SampleIdsLocal(s)),g)}.collectAsMap
      val vsm1PrimeExpandedMap = vsm1Prime.expand().map{case (v,s,g) => ((v,vsm1PrimeSampleIdsLocal(s)),g)}.collectAsMap
      val vsm2ExpandedMap = vsm2.expand().map{case (v,s,g) => ((v,vsm2SampleIdsLocal(s)),g)}.collectAsMap
      val vsm2PrimeExpandedMap = vsm2Prime.expand().map{case (v,s,g) => ((v,vsm2PrimeSampleIdsLocal(s)),g)}.collectAsMap
      val mergedExpandedMap = mergedVSM.expand().map{case (v,s,g) => ((v,mergeSampleIdsLocal(s)),g)}.collectAsMap

      for (((v,s),gMerge) <- mergedExpandedMap) {
        val g1t = vsm1ExpandedMap.get((v, s))
        val g2t = vsm2ExpandedMap.get((v, s))

        (sjt,vjt) match {
          case ("inner","inner") => assert(gMerge == (g1t.get, g2t.get))
          case ("inner","left") => assert(gMerge == (g1t.get, g2t))
          case ("inner","right") => assert(gMerge == (g1t, g2t.get))
          case ("inner","outer") => assert(gMerge == (g1t, g2t))
          case ("left","inner") => assert(gMerge == (g1t.get, g2t))
          case ("left","left") => assert(gMerge == (g1t.get, g2t)) // fails on Some(Some(_))
          case ("left","right") => assert(gMerge == (g1t.get, g2t))
          case ("left","outer") => assert(gMerge == (g1t.get, g2t))
          case ("right","inner") => assert(gMerge == (g1t, g2t.get))
          case ("right","left") => assert(gMerge == (g1t, g2t.get))
          case ("right","right") => assert(gMerge == (g1t, g2t.get))
          case ("right","outer") => assert(gMerge == (g1t, g2t.get))
          case ("outer","inner") => assert(gMerge == (g1t, g2t))
          case ("outer","left") => assert(gMerge == (g1t, g2t))
          case ("outer","right") => assert(gMerge == (g1t, g2t))
          case ("outer","outer") => assert(gMerge == (g1t, g2t))




          //case "right" => assert(gMerge == (g1t, g2t.get))
          //case "outer" => assert(gMerge == (g1t, g2t))
        }
//        assert(gMerge == (g1,g2)) // this keeps failing because I can't get rid of the options in gMerge (type = product of serializable)
      }
    }
  }
}

