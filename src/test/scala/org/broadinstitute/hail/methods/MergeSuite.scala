package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.broadinstitute.hail.variant.GenotypeType._
import org.testng.annotations.Test
import org.broadinstitute.hail.Utils._

class MergeSuite extends SparkSuite {
  @Test def test() = {

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

    val trueSampleConc: Map[Int, Option[Double]] = Map(0 -> Some(0.0), 1 -> Some(0.3333), 2 -> Some(0.75), 3 -> Some(0.5), 4 -> Some(1.0))
    val trueVariantConc: Map[String, Option[Double]] = Map("1:0:A:T" -> Some(0.6), "1:1:A:T" -> Some(1.0), "1:2:A:T" -> Some(0.5), "1:3:A:T" -> None, "1:4:A:T" -> Some(0.25))
    val rdd1 = TestRDDBuilder.buildRDD(5, 5, sc, "tuple", gtArray = Some(gt1))
    val rdd2 = TestRDDBuilder.buildRDD(5, 5, sc, "tuple", gtArray = Some(gt2))

    val mergedVds = Merge(rdd1, rdd2, sc)

    // Check sample concordance
    val sampleConcordance = mergedVds.sampleConcordance.collectAsMap()
    sampleConcordance.foreach { case (s, ct) =>
      assert(optionCloseEnough(trueSampleConc.get(s).get, ct.calcConcordance))
    }


    // Check variant concordance
    val variantConcordance = mergedVds.variantConcordance.collectAsMap()
    variantConcordance.foreach { case (v, ct) =>
      assert(optionCloseEnough(trueVariantConc.get(v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt).get, ct.calcConcordance))
    }


    // Check merged RDD has correct numbers for merge mode 1
    val mergeTruth1: Map[(String, Int), GenotypeType] = Map(("1:0:A:T", 0) -> NoCall, ("1:0:A:T", 1) -> NoCall, ("1:0:A:T", 2) -> HomVar, ("1:0:A:T", 3) -> Het, ("1:0:A:T", 4) -> HomRef,
      ("1:1:A:T", 0) -> HomVar, ("1:1:A:T", 1) -> Het, ("1:1:A:T", 2) -> HomRef, ("1:1:A:T", 3) -> Het, ("1:1:A:T", 4) -> HomRef,
      ("1:2:A:T", 0) -> NoCall, ("1:2:A:T", 1) -> NoCall, ("1:2:A:T", 2) -> Het, ("1:2:A:T", 3) -> Het, ("1:2:A:T", 4) -> HomVar,
      ("1:3:A:T", 0) -> HomVar, ("1:3:A:T", 1) -> NoCall, ("1:3:A:T", 2) -> Het, ("1:3:A:T", 3) -> HomRef, ("1:3:A:T", 4) -> HomRef,
      ("1:4:A:T", 0) -> NoCall, ("1:4:A:T", 1) -> Het, ("1:4:A:T", 2) -> NoCall, ("1:4:A:T", 3) -> NoCall, ("1:4:A:T", 4) -> Het)

    val mergedResult1 = mergedVds.applyMergeMode(1).mapValues(gt => gt match {
      case Some(gt) => gt.gtType
      case _ => NoCall
    }
    ).collectAsMap()

    mergedResult1.foreach { case ((v, s), gt) => assert(mergeTruth1.get((v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt, s)).get == gt) }



    // Check merged RDD has correct numbers for merge mode 2
    val mergeTruth2: Map[(String, Int), GenotypeType] = Map(("1:0:A:T", 0) -> HomRef, ("1:0:A:T", 1) -> Het, ("1:0:A:T", 2) -> HomVar, ("1:0:A:T", 3) -> Het, ("1:0:A:T", 4) -> HomRef,
      ("1:1:A:T", 0) -> HomVar, ("1:1:A:T", 1) -> Het, ("1:1:A:T", 2) -> HomRef, ("1:1:A:T", 3) -> Het, ("1:1:A:T", 4) -> HomRef,
      ("1:2:A:T", 0) -> HomVar, ("1:2:A:T", 1) -> Het, ("1:2:A:T", 2) -> Het, ("1:2:A:T", 3) -> Het, ("1:2:A:T", 4) -> HomVar,
      ("1:3:A:T", 0) -> HomVar, ("1:3:A:T", 1) -> NoCall, ("1:3:A:T", 2) -> Het, ("1:3:A:T", 3) -> HomRef, ("1:3:A:T", 4) -> HomRef,
      ("1:4:A:T", 0) -> HomRef, ("1:4:A:T", 1) -> Het, ("1:4:A:T", 2) -> HomVar, ("1:4:A:T", 3) -> HomVar, ("1:4:A:T", 4) -> Het)

    val mergedResult2 = mergedVds.applyMergeMode(2).mapValues(gt => gt match {
      case Some(gt) => gt.gtType
      case _ => NoCall
    }
    ).collectAsMap()

    mergedResult2.foreach { case ((v, s), gt) => assert(mergeTruth2.get((v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt, s)).get == gt) }



    // Check merged RDD has correct numbers for merge mode 3
    val mergeTruth3: Map[(String, Int), GenotypeType] = Map(("1:0:A:T", 0) -> HomVar, ("1:0:A:T", 1) -> HomRef, ("1:0:A:T", 2) -> HomVar, ("1:0:A:T", 3) -> Het, ("1:0:A:T", 4) -> HomRef,
      ("1:1:A:T", 0) -> HomVar, ("1:1:A:T", 1) -> Het, ("1:1:A:T", 2) -> HomRef, ("1:1:A:T", 3) -> Het, ("1:1:A:T", 4) -> HomRef,
      ("1:2:A:T", 0) -> HomRef, ("1:2:A:T", 1) -> HomVar, ("1:2:A:T", 2) -> Het, ("1:2:A:T", 3) -> Het, ("1:2:A:T", 4) -> HomVar,
      ("1:3:A:T", 0) -> HomVar, ("1:3:A:T", 1) -> NoCall, ("1:3:A:T", 2) -> Het, ("1:3:A:T", 3) -> HomRef, ("1:3:A:T", 4) -> HomRef,
      ("1:4:A:T", 0) -> Het, ("1:4:A:T", 1) -> Het, ("1:4:A:T", 2) -> Het, ("1:4:A:T", 3) -> Het, ("1:4:A:T", 4) -> Het)

    val mergedResult3 = mergedVds.applyMergeMode(3).mapValues(gt => gt match {
      case Some(gt) => gt.gtType
      case _ => NoCall
    }
    ).collectAsMap()

    mergedResult3.foreach { case ((v, s), gt) => assert(mergeTruth3.get((v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt, s)).get == gt) }



    // Check merged RDD has correct numbers for merge mode 4
    val mergeTruth4: Map[(String, Int), GenotypeType] = Map(("1:0:A:T", 0) -> HomRef, ("1:0:A:T", 1) -> Het, ("1:0:A:T", 2) -> HomVar, ("1:0:A:T", 3) -> Het, ("1:0:A:T", 4) -> HomRef,
      ("1:1:A:T", 0) -> NoCall, ("1:1:A:T", 1) -> Het, ("1:1:A:T", 2) -> HomRef, ("1:1:A:T", 3) -> Het, ("1:1:A:T", 4) -> HomRef,
      ("1:2:A:T", 0) -> HomVar, ("1:2:A:T", 1) -> Het, ("1:2:A:T", 2) -> Het, ("1:2:A:T", 3) -> Het, ("1:2:A:T", 4) -> HomVar,
      ("1:3:A:T", 0) -> HomVar, ("1:3:A:T", 1) -> NoCall, ("1:3:A:T", 2) -> Het, ("1:3:A:T", 3) -> NoCall, ("1:3:A:T", 4) -> HomRef,
      ("1:4:A:T", 0) -> HomRef, ("1:4:A:T", 1) -> Het, ("1:4:A:T", 2) -> HomVar, ("1:4:A:T", 3) -> HomVar, ("1:4:A:T", 4) -> NoCall)

    val mergedResult4 = mergedVds.applyMergeMode(4).mapValues(gt => gt match {
      case Some(gt) => gt.gtType
      case _ => NoCall
    }
    ).collectAsMap()

    mergedResult4.foreach { case ((v, s), gt) => assert(mergeTruth4.get((v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt, s)).get == gt) }



    // Check merged RDD has correct numbers for merge mode 4
    val mergeTruth5: Map[(String, Int), GenotypeType] = Map(("1:0:A:T", 0) -> HomVar, ("1:0:A:T", 1) -> HomRef, ("1:0:A:T", 2) -> HomVar, ("1:0:A:T", 3) -> Het, ("1:0:A:T", 4) -> HomRef,
      ("1:1:A:T", 0) -> HomVar, ("1:1:A:T", 1) -> NoCall, ("1:1:A:T", 2) -> HomRef, ("1:1:A:T", 3) -> NoCall, ("1:1:A:T", 4) -> HomRef,
      ("1:2:A:T", 0) -> HomRef, ("1:2:A:T", 1) -> HomVar, ("1:2:A:T", 2) -> Het, ("1:2:A:T", 3) -> NoCall, ("1:2:A:T", 4) -> HomVar,
      ("1:3:A:T", 0) -> NoCall, ("1:3:A:T", 1) -> NoCall, ("1:3:A:T", 2) -> NoCall, ("1:3:A:T", 3) -> HomRef, ("1:3:A:T", 4) -> NoCall,
      ("1:4:A:T", 0) -> Het, ("1:4:A:T", 1) -> Het, ("1:4:A:T", 2) -> Het, ("1:4:A:T", 3) -> Het, ("1:4:A:T", 4) -> Het)

    val mergedResult5 = mergedVds.applyMergeMode(5).mapValues(gt => gt match {
      case Some(gt) => gt.gtType
      case _ => NoCall
    }
    ).collectAsMap()

    mergedResult5.foreach { case ((v, s), gt) => assert(mergeTruth5.get((v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt, s)).get == gt) }


    // Check merged RDD when only some samples overlap
    val trueSampleConc2: Map[Int, Option[Double]] = Map(0 -> Some(0.66666), 1 -> Some(1.000), 2 -> Some(0.75))
    val trueVariantConc2: Map[String, Option[Double]] = Map("1:0:A:T" -> Some(1.0), "1:1:A:T" -> Some(1.0), "1:2:A:T" -> Some(1.0), "1:3:A:T" -> None, "1:4:A:T" -> Some(0.3333))
    val rdd3 = TestRDDBuilder.buildRDD(5, 5, sc, "tuple", gtArray = Some(gt1), sampleIds=Some(Array("s1", "s2", "s3", "s4", "s5")))
    val rdd4 = TestRDDBuilder.buildRDD(5, 5, sc, "tuple", gtArray = Some(gt2), sampleIds=Some(Array("s6", "s7", "s3", "s2", "s1")))

    val mergedVds2 = Merge(rdd3, rdd4, sc)

    // Check sample concordance
    val sampleConcordance2 = mergedVds2.sampleConcordance.collectAsMap()
    sampleConcordance2.foreach { case (s, ct) =>
      assert(optionCloseEnough(trueSampleConc2.get(s).get, ct.calcConcordance))
    }


    // Check variant concordance
    val variantConcordance2 = mergedVds2.variantConcordance.collectAsMap()
    variantConcordance2.foreach { case (v, ct) =>
      assert(optionCloseEnough(trueVariantConc2.get(v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt).get, ct.calcConcordance))
    }
  }
}