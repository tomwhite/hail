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
      Array(2, 0, 2, 1, 0,1),
      Array(2, -1, 0, -1, 0,1),
      Array(0, 2, 1, -1, 2,1),
      Array(-1, -1, -1, 0, -1,1),
      Array(1, 1, 1, 1, 1,1))

    val vsm1 = TestRDDBuilder.buildRDD(5, 1, sc,sampleIds=Some(Array("s1", "s2", "s3", "s4", "s5")))
    val vsm2 = TestRDDBuilder.buildRDD(5, 1, sc,sampleIds=Some(Array("foo", "s7", "beet", "pug", "s3")))

    val expectedNumSamples = Map("inner" -> 1, "left" -> 5, "right" -> 5, "outer" -> 9)
    val expectedNumVariants = Map("inner" -> 1, "left" -> 1, "right" -> 1, "outer" -> 1)

    //val joinTypes = Array("inner","left","right","outer")
    val joinTypes = Array("inner")
    for (sjt <- joinTypes; vjt <- joinTypes) {
      val nSamples = expectedNumSamples(sjt)
      val nVariants = expectedNumVariants(vjt)

      val (vsm1Prime,vsm2Prime) = sjt match {
        case "inner" => vsm1.reindexSamplesInnerJoin(vsm2)
        case "left" => vsm1.reindexSamplesLeftJoin(vsm2)
        case "right" => vsm1.reindexSamplesRightJoin(vsm2)
        case "outer" => vsm1.reindexSamplesOuterJoin(vsm2)
        case _ => throw new UnsupportedOperationException
      }

      val mergedVSM = vjt match {
        case "inner" => vsm1Prime.innerJoin(vsm2Prime)
        case "left" => vsm1Prime.leftOuterJoin(vsm2Prime)
        case "right" => vsm1Prime.rightOuterJoin(vsm2Prime)
        case "outer" => vsm1Prime.fullOuterJoin(vsm2Prime)
        case _ => throw new UnsupportedOperationException
      }

      assert(mergedVSM.localSamples.length == nSamples)
      assert(mergedVSM.rdd.filter{case (v,g) => g.size == nSamples}.count == nVariants)

      val vsm1SampleIdsLocal = vsm1.sampleIds
      val vsm2SampleIdsLocal = vsm2.sampleIds
      val vsm1PrimeSampleIdsLocal = vsm1Prime.sampleIds
      val vsm2PrimeSampleIdsLocal = vsm2Prime.sampleIds
      val mergeSampleIdsLocal = mergedVSM.sampleIds

      val mergedExpandedMap = mergedVSM.expand().map{case (v,s,g) => ((v,mergeSampleIdsLocal(s)),g)}.collectAsMap
      val vsm1ExpandedMap = vsm1.expand().map{case (v,s,g) => ((v,vsm1SampleIdsLocal(s)),g)}.collectAsMap
      val vsm2ExpandedMap = vsm2.expand().map{case (v,s,g) => ((v,vsm2SampleIdsLocal(s)),g)}.collectAsMap
      val vsm1PrimeExpandedMap = vsm1Prime.expand().map{case (v,s,g) => ((v,vsm1PrimeSampleIdsLocal(s)),g)}.collectAsMap
      val vsm2PrimeExpandedMap = vsm2Prime.expand().map{case (v,s,g) => ((v,vsm2PrimeSampleIdsLocal(s)),g)}.collectAsMap

      println(mergedExpandedMap.mkString("\n"))
      println("-----------------------------------")
      println("-----------------------------------")
      println(vsm1ExpandedMap.mkString("\n"))
      println("-----------------------------------")
      println(vsm1PrimeExpandedMap.mkString("\n"))
      println("-----------------------------------")
      println("-----------------------------------")
      println(vsm2ExpandedMap.mkString("\n"))
      println("-----------------------------------")
      println(vsm2PrimeExpandedMap.mkString("\n"))
      //println(mergedExpandedMap.mkString("\n"))

      for (((v,s),gMerge) <- mergedExpandedMap){
        val g1 = vsm1ExpandedMap.getOrElse((v,s),None)
        val g2 = vsm2ExpandedMap.getOrElse((v,s),None)
        //assert(gMerge == (g1,g2))
      }

    /*  mergedVSM.rdd.map{case (v,(a,b)) =>
        val aOriginal = vsm1Variants.indexOf(v) match {
          case -1 => None
          case _ => vsm1.rdd.filter{case (v1,g1) => v1 == v}.map{case (v1,g1) => g1}.collect().(0)
        }
        val bOriginal = vsm2Variants.indexOf(v) match {
          case -1 => None
          case _ => vsm2.rdd.filter{case (v2,g2) => v2 == v}.map{case (v2,g2) => g2}.collect().(0)
        }

      }*/

      // need a test to make sure genotype is same after merge
    }
  }
}

