package org.broadinstitute.hail.methods

import org.apache.spark.util.StatCounter
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.driver._
import org.testng.annotations.Test


class AnnotateGlobalSuite extends SparkSuite {
  @Test def test(): Unit = {

    var s = SplitMulti.run(State(sc, sqlContext, LoadVCF(sc, "src/test/resources/sample2.vcf")), Array.empty[String])
    s = VariantQC.run(s, Array.empty[String])
    s = SampleQC.run(s, Array.empty[String])

    s = AnnotateGlobal.run(s, Array("expr", "-c", "global.mafDist = variants.stats(va.qc.MAF), global.singStats = samples.count(sa.qc.nSingleton > 2)"))
    s = AnnotateGlobal.run(s, Array("expr", "-c", "global.anotherAnnotation.sumOver2 = global.mafDist.sum / 2"))
    s = AnnotateGlobal.run(s, Array("expr", "-c", "global.macDist = variants.stats(va.qc.MAC)"))
    s = AnnotateGlobal.run(s, Array("expr", "-c", "global.CRStats = samples.stats(sa.qc.callRate)"))


    val vds = s.vds
    val qSingleton = vds.querySA("sa.qc.nSingleton")._2
    val qSingletonGlobal = vds.queryGlobal("global.singStats")._2

    val sCount = vds.sampleAnnotations.count(sa => {
      qSingleton(sa).exists(_.asInstanceOf[Int] > 2)
    })

    assert(qSingletonGlobal.contains(sCount))

    val qMaf = vds.queryVA("va.qc.MAF")._2
    val mafSC = vds.variantsAndAnnotations.map(_._2)
        .aggregate(new StatCounter())({case (statC, va) =>
        val maf = qMaf(va)
        maf.foreach(o => statC.merge(o.asInstanceOf[Double]))
        statC
        }, { case (sc1, sc2) => sc1.merge(sc2)})

    assert(vds.queryGlobal("global.mafDist")._2
      .contains(Annotation(mafSC.mean, mafSC.stdev, mafSC.min, mafSC.max, mafSC.count, mafSC.sum)))

    assert(vds.queryGlobal("global.anotherAnnotation.sumOver2")._2.contains(mafSC.sum / 2))

    val qMac = vds.queryVA("va.qc.MAC")._2
    val macSC = vds.variantsAndAnnotations.map(_._2)
      .aggregate(new StatCounter())({case (statC, va) =>
        val mac = qMac(va)
        mac.foreach(o => statC.merge(o.asInstanceOf[Int]))
        statC
      }, { case (sc1, sc2) => sc1.merge(sc2)})

    assert(vds.queryGlobal("global.macDist")._2
      .contains(Annotation(macSC.mean, macSC.stdev, macSC.min.toInt,
        macSC.max.toInt, macSC.count, macSC.sum.round.toInt)))

    val qCR = vds.querySA("sa.qc.callRate")._2
    val crSC = vds.sampleAnnotations
      .aggregate(new StatCounter())({case (statC, sa) =>
        val cr = qCR(sa)
        cr.foreach(o => statC.merge(o.asInstanceOf[Double]))
        statC
      }, { case (sc1, sc2) => sc1.merge(sc2)})

    assert(vds.queryGlobal("global.CRStats")._2
      .contains(Annotation(crSC.mean, crSC.stdev, crSC.min,
        crSC.max, crSC.count, crSC.sum)))

  }

  @Test def testLists() {
    val out1 = tmpDir.createTempFile("file1", ".txt")
    val out2 = tmpDir.createTempFile("file2", ".txt")

    val toWrite1 = Array("Gene1", "Gene2", "Gene3", "Gene4", "Gene5")
    val toWrite2 = Array("1", "5", "4", "2", "2")

    writeTextFile(out1, sc.hadoopConfiguration) { out =>
      toWrite1.foreach { line =>
        out.write(line + "\n")
      }
    }

    writeTextFile(out2, sc.hadoopConfiguration) { out =>
      toWrite2.foreach { line =>
        out.write(line + "\n")
      }
    }

    var s = State(sc, sqlContext)
    s = ImportVCF.run(s, Array("src/test/resources/sample.vcf"))
    s = AnnotateGlobal.run(s, Array("list", "-i", out1, "-r", "global.geneList", "--as-set"))
    s = AnnotateGlobal.run(s, Array("list", "-i", out2, "-r", "global.array"))

    val (_, anno1) = s.vds.queryGlobal("global.geneList")
    val (_, anno2) = s.vds.queryGlobal("global.array")
    assert(anno1.contains(toWrite1.toSet))
    assert(anno2.contains(toWrite2: IndexedSeq[Any]))
  }
}
