package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.driver.{ExportVariants, ExportSamples, Concordance, State}
import org.broadinstitute.hail.variant.Genotype
import org.testng.annotations.Test
import scala.io.Source

class ConcordanceSuite extends SparkSuite {
  @Test def testConcordanceTable() = {
    val ct1 = new ConcordanceTable

    ct1.addCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0))), 1000)
    ct1.addCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 200)
    ct1.addCount(Some(Genotype(2, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 10)
    ct1.addCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0))), 20)
    ct1.addCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 50)
    ct1.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(-1, (0, 0), 0, (0, 0, 0))))
    ct1.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0))), 5)
    ct1.addCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), 8)
    ct1.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 30)
    ct1.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 15)
    ct1.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 9)


    assert(ct1.numMismatches == 70)
    assert(ct1.numMatches == 1210)
    assert(ct1.numNoCall == 68)
    assert(ct1.numTotal == 1348)
    assert(D_==(ct1.discordanceRate.get, 0.0546875))
    assert(D_==(ct1.concordanceRate.get, 0.9453125))
    assert(ct1.getCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0)))) == 1000)

    val ct2 = new ConcordanceTable

    ct2.addCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0))), 1501)
    ct2.addCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 201)
    ct2.addCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 1)
    ct2.addCount(Some(Genotype(2, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 30)
    ct2.addCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0))), 25)
    ct2.addCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 55)
    ct2.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(-1, (0, 0), 0, (0, 0, 0))))
    ct2.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0))), 16)
    ct2.addCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), 17)
    ct2.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0))), 20)
    ct2.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 12)
    ct2.addCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 8)
    ct2.addCount(Some(Genotype(2, (0, 0), 0, (0, 0, 0))), Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), 5)
    ct2.addCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0))), 25)

    assert(ct2.getCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0)))) == 20)


    val ct3 = ct1.merge(ct2)

    assert(ct3.getCount(Some(Genotype(0, (0, 0), 0, (0, 0, 0))), Some(Genotype(0, (0, 0), 0, (0, 0, 0)))) == 2501)
    assert(ct3.getCount(Some(Genotype(1, (0, 0), 0, (0, 0, 0))), Some(Genotype(1, (0, 0), 0, (0, 0, 0)))) == 402)
    assert(ct3.getCount(Some(Genotype(-1, (0, 0), 0, (0, 0, 0))), Some(Genotype(2, (0, 0), 0, (0, 0, 0)))) == 44)
    assert(ct3.getCount(Some(Genotype(2, (0, 0), 0, (0, 0, 0))), Some(Genotype(-1, (0, 0), 0, (0, 0, 0)))) == 5)
  }

  @Test def testConcordanceResults() = {

    val overallConc = 0.81307
    val numCalledBoth = 33435
    val numTotal = 34600
    val numConcordant = 27185

    val truthSampleConcordance = "src/test/resources/sampleConcordanceTruthData.txt"
    val truthVariantConcordance = "src/test/resources/variantConcordanceTruthData.txt"
    val vcf1 = "src/test/resources/multipleChromosomes.vcf"
    val vcf2 = "src/test/resources/multipleChromosomesIdShuffle.vcf"

    val vds1 = LoadVCF(sc, vcf1, nPartitions = Some(10))
    val vds2 = LoadVCF(sc, vcf2, nPartitions = Some(10))
    val state = State(sc, sqlContext, vds = vds1, vds2 = Some(vds2))

    val state2 = Concordance.run(state, Array("-s"))

    val sampleExportItems = "s.id, " +
      "sa.conc.HomRef_HomRef, sa.conc.HomRef_Het, sa.conc.HomRef_HomAlt, sa.conc.HomRef_NoCall, sa.conc.HomRef_None, " +
      "sa.conc.Het_HomRef, sa.conc.Het_Het, sa.conc.Het_HomAlt, sa.conc.Het_NoCall, sa.conc.Het_None, " +
      "sa.conc.HomAlt_HomRef, sa.conc.HomAlt_Het, sa.conc.HomAlt_HomAlt, sa.conc.HomAlt_NoCall, sa.conc.HomAlt_None, " +
      "sa.conc.NoCall_HomRef, sa.conc.NoCall_Het, sa.conc.NoCall_HomAlt, sa.conc.NoCall_NoCall, sa.conc.NoCall_None, " +
      "sa.conc.None_HomRef, sa.conc.None_Het, sa.conc.None_HomAlt, sa.conc.None_NoCall, sa.conc.None_None"

    val variantExportItems = "v, " +
      "va.conc.HomRef_HomRef, va.conc.HomRef_Het, va.conc.HomRef_HomAlt, va.conc.HomRef_NoCall, va.conc.HomRef_None, " +
      "va.conc.Het_HomRef, va.conc.Het_Het, va.conc.Het_HomAlt, va.conc.Het_NoCall, va.conc.Het_None, " +
      "va.conc.HomAlt_HomRef, va.conc.HomAlt_Het, va.conc.HomAlt_HomAlt, va.conc.HomAlt_NoCall, va.conc.HomAlt_None, " +
      "va.conc.NoCall_HomRef, va.conc.NoCall_Het, va.conc.NoCall_HomAlt, va.conc.NoCall_NoCall, va.conc.NoCall_None, " +
      "va.conc.None_HomRef, va.conc.None_Het, va.conc.None_HomAlt, va.conc.None_NoCall, va.conc.None_None"

    ExportSamples.run(state2, Array("-c", sampleExportItems, "-o", "/tmp/sampleConcordance.txt"))
    ExportVariants.run(state2, Array("-c", variantExportItems, "-o", "/tmp/variantConcordance.txt"))

    // read in sampleConcordance and compare to truth data
    // hail output has no header
    val hailSampleOutput = Source.fromFile("/tmp/sampleConcordance.txt").getLines.map(_.split("\t")).map(data => (data(0),data.drop(1).mkString("\t"))).toMap
    val pythonSampleOutput = Source.fromFile(truthSampleConcordance).getLines.drop(1).map(_.split("\t")).map(data => (data(0),data.drop(1).mkString("\t"))).toMap
    assert(hailSampleOutput.forall{case (id, data) => data == pythonSampleOutput(id)})
    assert(pythonSampleOutput.forall{case (id, data) => data == hailSampleOutput(id)})

    // read in variantConcordance and compare to truth data
    // hail output has no header
    val hailVariantOutput = Source.fromFile("/tmp/variantConcordance.txt").getLines.map(_.split("\t")).map(data => (data(0),data.drop(1).mkString("\t"))).toMap
    val pythonVariantOutput = Source.fromFile(truthVariantConcordance).getLines.drop(1).map(_.split("\t")).map(data => (data(0),data.drop(1).mkString("\t"))).toMap
    assert(hailVariantOutput.forall{case (id, data) => data == pythonVariantOutput(id)})
    assert(pythonVariantOutput.forall{case (id, data) => data == hailVariantOutput(id)})
  }
}
