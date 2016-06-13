package org.broadinstitute.hail.variant

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.driver._
import org.testng.annotations.Test
import org.broadinstitute.hail.check.Gen._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._

class PPSuite extends SparkSuite {

  def readVariantQCFile(fileName: String) = sc.parallelize(readLines(fileName, sc.hadoopConfiguration)(_.map(_.transform { line =>
    val Array(chr, pos, ref, alt, callRate, mac, maf, nCalled,
    nNotCalled, nHomRef, nHet, nHomVar, dpMean, dpStDev, gqMean,
    gqStDev, nNonRef, rHet, rHetHomVar, rExpHetFreq, pHWE, infoScore) = line.value.split("\\s+")
    infoScore
  }
  ).toArray.drop(1)))

  //  @Test def test1() {
//    var s = State(sc, sqlContext, null)
//    val testVCF = "src/test/resources/sample.vcf"
//    //val outVCF = tmpDir.createTempFile("testPriorExpr",".vcf")
//    val outRoot = tmpDir.createTempFile("testPriorExpr")
//    s = ImportVCF.run(s, Array(testVCF))
//    s = SplitMulti.run(s, Array.empty[String])
//    s = AnnotateVariantsExpr.run(s, Array("-c", "va.prior = uniformPrior(v)"))
//    s = PrintSchema.run(s, Array("--va"))
//    //println(s.vds.variantsAndAnnotations.collect().mkString(","))
//    //s = AnnotateVariantsExpr.run(s, Array("-c", "va.prior = va.info.pg"))
//    s = VariantQC.run(s, Array("-o", outRoot + ".vqc.txt"))
//    s = ExportGEN.run(s, Array("-o", outRoot + ".exportgen"))
////    FilterGenotypes.run(s, Array("--remove", "-c", "g.pl[0] != 0"))
////    FilterGenotypes.run(s, Array("--remove", "-c", "g.pl()[0] != 0"))
////    FilterGenotypes.run(s, Array("--remove", "-c", "g.gp[0] != 0"))
////    FilterGenotypes.run(s, Array("--remove", "-c", "g.gp()[0] != 0"))
////    FilterGenotypes.run(s, Array("--remove", "-c", "g.pp[0] != 0"))
////    FilterGenotypes.run(s, Array("--remove", "-c", "g.pp()[0] != 0"))
////    val res = FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(v))[0] != 0"))
////    .vds.rdd.collect()
////    println(res.length)
////    println(res.head)
//
////    val res = FilterGenotypes.run(s, Array("--remove", "-c", "uniformPrior(v)[0] != 0"))
////      .vds.rdd.collect()
////    println(res.length)
////    println(res.head)
//
//    //FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(5))[0] != 0"))
//    //FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(v))[0] != 0"))
//    //FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(v, v, v))[0] != 0"))
////    ExportVCF.run(s, Array("-o",outVCF))
//  }

  @Test def testNoPrior() {
    var s = State(sc, sqlContext, null)
    val testVCF = "src/test/resources/sample.vcf"
    val outRoot = tmpDir.createTempFile("testNoPrior")
    s = ImportVCF.run(s, Array(testVCF))
    s = SplitMulti.run(s, Array.empty[String])
    s = VariantQC.run(s, Array("-o", outRoot + ".vqc.txt"))
    s = ExportGEN.run(s, Array("-o", outRoot + ".exportgen"))

    val varqcResult = readVariantQCFile(outRoot + ".vqc.txt").map{s => s == "NA"}.fold(true)(_ && _)

    assert(varqcResult)
  }

  @Test def testUniformPrior() {
    var s = State(sc, sqlContext, null)
    val testVCF = "src/test/resources/sample.vcf"
    val outRoot = tmpDir.createTempFile("testUnifPrior")
    s = ImportVCF.run(s, Array(testVCF))
    s = SplitMulti.run(s, Array.empty[String])
    s = AnnotateVariantsExpr.run(s, Array("-c", "va.prior = uniformPrior(v)"))
    s = VariantQC.run(s, Array("-o", outRoot + ".vqc.txt"))
    s = ExportGEN.run(s, Array("-o", outRoot + ".exportgen"))

    val varqcResult = readVariantQCFile(outRoot + ".vqc.txt").map{s => s != "NA"}.fold(true)(_ && _)

    assert(varqcResult)
    // assert properties of va.prior

    // assert exportgen gives non- (0, 0, 0) answers
  }

//  @Test def testNonunifPrior() {
//    var s = State(sc, sqlContext, null)
//    val testVCF = "src/test/resources/sample.vcf"
//    val outRoot = tmpDir.createTempFile("testNonUnifPrior")
//    s = ImportVCF.run(s, Array(testVCF))
//    s = SplitMulti.run(s, Array.empty[String])
//    val signature = TArray[TInt]
//    val (newVAS, insertPrior) = s.vds.insertVA(signature, "prior")
//    s = s.vds.copy(rdd = s.vds.rdd.map{case (v, va, gs) => Genotype.renormPhredScale(buildableOfN[Array[Int], Int](v.nGenotypes, choose(0, 1000)).sample())})
//    s = VariantQC.run(s, Array("-o", outRoot + ".vqc.txt"))
//    s = ExportGEN.run(s, Array("-o", outRoot + ".exportgen"))
//
//    val varqcResult = readVariantQCFile(outRoot + ".vqc.txt").map{s => s != "NA"}.fold(true)(_ && _)
//
//    assert(varqcResult)
//  }
}
