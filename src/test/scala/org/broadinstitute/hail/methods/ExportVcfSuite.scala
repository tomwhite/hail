package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.driver._
import org.broadinstitute.hail.expr.TStruct
import org.broadinstitute.hail.variant.{Genotype, VariantSampleMatrix}
import org.testng.annotations.Test
import scala.language.postfixOps
import scala.sys.process._

import scala.io.Source

class ExportVcfSuite extends SparkSuite {

  @Test def testSameAsOrigBGzip() {
    val vcfFile = "src/test/resources/multipleChromosomes.vcf"
    val outFile = tmpDir.createTempFile("export", ".vcf")

    val vdsOrig = LoadVCF(sc, vcfFile, nPartitions = Some(10))
    val stateOrig = State(sc, sqlContext, vdsOrig)

    ExportVCF.run(stateOrig, Array("-o", outFile, "--export-pl"))

    val vdsNew = LoadVCF(sc, outFile, nPartitions = Some(10))

    assert(vdsOrig.same(vdsNew))
  }

  @Test def testSameAsOrigNoCompression() {
    val vcfFile = "src/test/resources/multipleChromosomes.vcf"
    val outFile = tmpDir.createTempFile("export", ".vcf")
    val outFile2 = tmpDir.createTempFile("export2", ".vcf")

    val vdsOrig = LoadVCF(sc, vcfFile, nPartitions = Some(10))
    val stateOrig = State(sc, sqlContext, vdsOrig)

    ExportVCF.run(stateOrig, Array("-o", outFile, "--export-pl"))

    val vdsNew = LoadVCF(sc, outFile, nPartitions = Some(10))

    assert(vdsOrig.eraseSplit.same(vdsNew.eraseSplit))

    val infoSize = vdsNew.vaSignature.getAsOption[TStruct]("info").get.size
    val toAdd = Some(Annotation.fromSeq(Array.fill[Any](infoSize)(null)))
    val (_, inserter) = vdsNew.insertVA(null, "info")

    val vdsNewMissingInfo = vdsNew.mapAnnotations((v, va, gs) => inserter(va, toAdd))

    ExportVCF.run(stateOrig.copy(vds = vdsNewMissingInfo), Array("-o", outFile2, "--export-pl"))

    assert(LoadVCF(sc, outFile2).eraseSplit.same(vdsNewMissingInfo.eraseSplit))
  }

  @Test def testSorted() {
    val vcfFile = "src/test/resources/multipleChromosomes.vcf"
    val outFile = tmpDir.createTempFile("sort", ".vcf.bgz")

    val vdsOrig = LoadVCF(sc, vcfFile, nPartitions = Some(10))
    val stateOrig = State(sc, sqlContext, vdsOrig)

    ExportVCF.run(stateOrig, Array("-o", outFile, "--export-pl"))

    val vdsNew = LoadVCF(sc, outFile, nPartitions = Some(10))
    val stateNew = State(sc, sqlContext, vdsNew)

    case class Coordinate(contig: String, start: Int, ref: String, alt: String) extends Ordered[Coordinate] {
      def compare(that: Coordinate) = {
        if (this.contig != that.contig)
          this.contig.compareTo(that.contig)
        else if (this.start != that.start)
          this.start.compareTo(that.start)
        else if (this.ref != that.ref)
          this.ref.compareTo(that.ref)
        else
          this.alt.compareTo(that.alt)
      }
    }
    val coordinates: Array[Coordinate] = readFile(outFile, stateNew.hadoopConf) { s =>
      Source.fromInputStream(s)
        .getLines()
        .filter(line => !line.isEmpty && line(0) != '#')
        .map(line => line.split("\t")).take(5).map(a => new Coordinate(a(0), a(1).toInt, a(3), a(4))).toArray
    }

    val sortedCoordinates = coordinates.sortWith { case (c1, c2) => c1.compare(c2) < 0 }

    assert(sortedCoordinates.sameElements(coordinates))
  }

  @Test def testReadWrite() {
    val s = State(sc, sqlContext, null)
    val out = tmpDir.createTempFile("foo", ".vcf")
    val out2 = tmpDir.createTempFile("foo2", ".vcf")
    val p = forAll(VariantSampleMatrix.gen[Genotype](sc, Genotype.gen _)) { (vsm: VariantSampleMatrix[Genotype]) =>
      hadoopDelete("/tmp/foo.vcf", sc.hadoopConfiguration, recursive = true)
      ExportVCF.run(s.copy(vds = vsm), Array("-o", out, "--export-pl"))
      val vsm2 = ImportVCF.run(s, Array(out)).vds
      ExportVCF.run(s.copy(vds = vsm2), Array("-o", out2, "--export-pl"))
      val vsm3 = ImportVCF.run(s, Array(out2)).vds
      vsm2.same(vsm3)
    }

    p.check
  }

  @Test def testPPs() {
    var s = State(sc, sqlContext)
    s = ImportVCF.run(s, Array("src/test/resources/sample.PPs.vcf"))
    val out = tmpDir.createTempFile("exportPPs", ".vcf")
    ExportVCF.run(s, Array("-o", out, "--export-pp"))

    val lines1 = readFile(out, sc.hadoopConfiguration) { in =>
      Source.fromInputStream(in)
        .getLines()
        .dropWhile(_.startsWith("#"))
        .toIndexedSeq
    }

    val lines2 = readFile("src/test/resources/sample.PPs.vcf", sc.hadoopConfiguration) { in =>
      Source.fromInputStream(in)
        .getLines()
        .dropWhile(_.startsWith("#"))
        .toIndexedSeq
    }

    assert(lines1 == lines2)
  }

  @Test def testExportFlags() {
    var s = State(sc, sqlContext, null)

    val testVCF = "src/test/resources/sample.vcf"
    val outPL = tmpDir.createTempFile("testExportPL", ".vcf")
    val outPP = tmpDir.createTempFile("testExportPP", ".vcf")
    val outGP = tmpDir.createTempFile("testExportGP", ".vcf")
    val outAll = tmpDir.createTempFile("testExportAll", ".vcf")

    s = ImportVCF.run(s, Array(testVCF))

    s = ExportVCF.run(s, Array("-o", outPL, "--export-pl"))
    s = ExportVCF.run(s, Array("-o", outPP, "--export-pp"))
    s = ExportVCF.run(s, Array("-o", outGP, "--export-gp"))
    s = ExportVCF.run(s, Array("-o", outAll, "--export-gp","--export-pl","--export-pp"))

    def read(fileName: String) = readFile(fileName, sc.hadoopConfiguration) { in =>
      Source.fromInputStream(in)
        .getLines()
        .toArray
    }

    def parseLines(lines: Array[String]) = {
      var headerMatch = scala.collection.mutable.Map("PL" -> false, "PP" -> false, "GP" -> false)
      var formatMatch = scala.collection.mutable.Map("PL" -> 0, "PP" -> 0, "GP" -> 0)
      var nLines = 0
      for (line <- lines) {
        if (line.startsWith("#")) {
          for (h <- headerMatch.keys){
            if (line.contains(h))
              headerMatch(h) = true
          }
        } else {
          nLines += 1
          val fields = line.split("\t")
          val formatField = fields(8).split(":").toSet
          for (f <- formatMatch.keys) {
            if (formatField.contains(f)) {
              formatMatch.update(f, formatMatch(f) + 1)
            }
          }
        }
      }
      (headerMatch, formatMatch, nLines)
    }

    val plResults = parseLines(read(outPL))
    val ppResults = parseLines(read(outPP))
    val gpResults = parseLines(read(outGP))
    val allResults = parseLines(read(outAll))

    assert(plResults._1("PL") && !plResults._1("PP") && !plResults._1("GP") &&
      plResults._2("PL") == plResults._3 && plResults._2("PP") == 0 && plResults._2("GP") == 0)

    assert(!ppResults._1("PL") && ppResults._1("PP") && !ppResults._1("GP") &&
      ppResults._2("PL") == 0 && ppResults._2("PP") == ppResults._3 && ppResults._2("GP") == 0)

    assert(!gpResults._1("PL") && !gpResults._1("PP") && gpResults._1("GP") &&
      gpResults._2("PL") == 0 && gpResults._2("PP") == 0 && gpResults._2("GP") == gpResults._3)

    assert(allResults._1("PL") && allResults._1("PP") && allResults._1("GP") &&
      allResults._2("PL") == allResults._3 && allResults._2("PP") == allResults._3 && allResults._2("GP") == allResults._3)
    sys.exit()

    var plState = State(sc, sqlContext, null)
    plState = ImportVCF.run(plState, Array(outPL))

    var ppState = State(sc, sqlContext, null)
    ppState = ImportVCF.run(ppState, Array(outPP))

    var gpState = State(sc, sqlContext, null)
    gpState = ImportVCF.run(gpState, Array(outGP))

    var allState = State(sc, sqlContext, null)
    allState = ImportVCF.run(allState, Array(outAll))

    //FIXME: move to ImportVCF suite
    assert(plState.vds.rdd.map{case (v, va, gs) => gs.forall{g => g.isPL}}.fold(true)(_ && _))
    assert(ppState.vds.rdd.map{case (v, va, gs) => gs.forall{g => g.isPP}}.fold(true)(_ && _))
    assert(gpState.vds.rdd.map{case (v, va, gs) => gs.forall{g => g.isGP}}.fold(true)(_ && _))
    sys.exit()
  }
}