package org.broadinstitute.hail.io

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.check.Gen._
import org.broadinstitute.hail.check.Prop._
import org.broadinstitute.hail.check.Properties
import org.broadinstitute.hail.driver._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.{FatalException, SparkSuite}
import org.testng.annotations.Test

import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

case class FilePaths(gen: String,
                     bgen: String,
                     sampleFile: String,
                     index: String,
                    numPartitions: Int = 1)

class LoadBgenSuite extends SparkSuite {

  def getNumberOfLinesInFile(file: String): Long = {
    readFile(file, sc.hadoopConfiguration) { s =>
      Source.fromInputStream(s)
        .getLines()
        .length
    }.toLong
  }

 /* @Test def testStatic {
    val fps = Array(
     /* FilePaths("/Users/jigold/failed_tests/bgen/testGenWriter.gen",
    "/Users/jigold/failed_tests/bgen/testGenWriter.bgen",
      "/Users/jigold/failed_tests/bgen/testGenWriter.sample",
    "/Users/jigold/failed_tests/bgen/testGenWriter.bgen.idx"),*/

/*      FilePaths("/Users/jigold/failed_tests/bgen/test1.gen",
        "/Users/jigold/failed_tests/bgen/test1.bgen",
        "/Users/jigold/failed_tests/bgen/test1.sample",
        "/Users/jigold/failed_tests/bgen/test1.bgen.idx", 6),

      FilePaths("/Users/jigold/failed_tests/bgen/test2.gen",
        "/Users/jigold/failed_tests/bgen/test2.bgen",
        "/Users/jigold/failed_tests/bgen/test2.sample",
        "/Users/jigold/failed_tests/bgen/test2.bgen.idx", 10),

      FilePaths("/Users/jigold/failed_tests/bgen/test3.gen",
        "/Users/jigold/failed_tests/bgen/test3.bgen",
        "/Users/jigold/failed_tests/bgen/test3.sample",
        "/Users/jigold/failed_tests/bgen/test3.bgen.idx", 3),*/

      FilePaths("/Users/jigold/failed_tests/bgen/test5.gen",
        "/Users/jigold/failed_tests/bgen/test5.bgen",
        "/Users/jigold/failed_tests/bgen/test5.sample",
        "/Users/jigold/failed_tests/bgen/test5.bgen.idx", 2)
    )

    for (fp <- fps) {
      hadoopDelete(fp.index, sc.hadoopConfiguration, true)

      val origVds = GenLoader(fp.gen, fp.sampleFile, sc)
      var q = IndexBGEN.run(State(sc, sqlContext, null), Array(fp.bgen))
      q = ImportBGEN.run(State(sc, sqlContext, null), Array("-s", fp.sampleFile, "-n", fp.numPartitions.toString, fp.bgen))
      val importedVds = q.vds

      assert(importedVds.nSamples == origVds.nSamples)
      assert(importedVds.nVariants == origVds.nVariants)
      assert(importedVds.sampleIds == origVds.sampleIds)

      val importedVariants = importedVds.variants
      val origVariants = origVds.variants

      val importedFull = importedVds.expandWithAll().map { case (v, va, s, sa, gt) => ((v, s), gt) }
      val originalFull = origVds.expandWithAll().map { case (v, va, s, sa, gt) => ((v, s), gt) }

      originalFull.fullOuterJoin(importedFull).foreach { case ((v, i), (gt1, gt2)) => if (gt1 != gt2) println(s"$v $i $gt1 $gt2") }
      assert(originalFull.fullOuterJoin(importedFull).map { case ((v, i), (gt1, gt2)) => gt1 == gt2 }.fold(true)(_ && _))

/*      originalFull.fullOuterJoin(importedFull).map{ case ((v, i), (gt1, gt2)) =>
        val gt1x = gt1 match {
          case Some(x) =>
            var newPl = x.pl.getOrElse(Array(0,0,0)).map{i => math.min(i, BgenLoader.MAX_PL)}
            val newGt = BgenUtils.parseGenotype(newPl)
            newPl = if (newGt == -1) null else newPl
            Some(x.copy(gt = Option(newGt), ad = None, dp = None, gq = None, pl = Option(newPl)))
          case None => None
        }
        if (gt1x == gt2)
          true
        else {
          if (gt1x.isDefined && gt2.isDefined)
            gt1x.get.pl.getOrElse(Array(BgenLoader.MAX_PL,BgenLoader.MAX_PL,BgenLoader.MAX_PL))
              .zip(gt2.get.pl.getOrElse(Array(BgenLoader.MAX_PL,BgenLoader.MAX_PL,BgenLoader.MAX_PL)))
              .forall { case (pl1, pl2) => math.abs(pl1 - pl2) <= 1 }
          else
            false
        }
      }.fold(true)(_ && _)*/
    }
  }*/

  /*@Test def testGavinExample() {
    val gen = "src/test/resources/example.gen"
    val sampleFile = "src/test/resources/example.sample"
    val bgen = "src/test/resources/example.v11.bgen"

    hadoopDelete(bgen + ".idx", sc.hadoopConfiguration, true)

    val nSamples = getNumberOfLinesInFile(sampleFile) - 2
    val nVariants = getNumberOfLinesInFile(gen)

    var s = State(sc, sqlContext, null)
    s = IndexBGEN.run(s, Array(bgen))
    s = ImportBGEN.run(s, Array("-s", sampleFile, "-n", "10", bgen))
    assert(s.vds.nSamples == nSamples && s.vds.nVariants == nVariants)

    val genVDS = GenLoader(gen, sampleFile, sc)
    val bgenVDS = s.vds
    val genVariantsAnnotations = genVDS.variantsAndAnnotations
    val bgenVariantsAnnotations = bgenVDS.variantsAndAnnotations

    val bgenQuery = bgenVDS.vaSignature.query("varid")
    val genQuery = genVDS.vaSignature.query("varid")
    val bgenFull = bgenVDS.expandWithAll().map{case (v, va, s, sa, gt) => ((bgenQuery(va).get,s),gt)}
    val genFull = genVDS.expandWithAll().map{case (v, va, s, sa, gt) => ((genQuery(va).get,s),gt)}

    assert(bgenVDS.metadata == genVDS.metadata)
    assert(bgenVDS.sampleIds == genVDS.sampleIds)
    assert(bgenVariantsAnnotations.collect() sameElements genVariantsAnnotations.collect())
    assert(genFull.fullOuterJoin(bgenFull).map{case ((v,i),(gt1,gt2)) => gt1 == gt2}.fold(true)(_ && _))
  }*/

  object Spec extends Properties("ImportBGEN") {
    val compGen = for (vds: VariantDataset <- VariantSampleMatrix.gen[Genotype](sc, Genotype.gen _);
                       nPartitions: Int <- choose(1, 10)) yield (vds, nPartitions)

    writeTextFile("/tmp/sample_rename.txt",sc.hadoopConfiguration) { case w =>
      w.write("NA\tfdsdakfasdkfla")
    }

    property("import generates same output as export") =
      forAll(compGen) { case (vds: VariantSampleMatrix[Genotype], nPartitions: Int) =>

        println(s"nPartitions=$nPartitions nSamples=${vds.nSamples} nVariants=${vds.nVariants}")
        val vdsRemapped = vds.copy(rdd = vds.rdd.map{case (v, va, gs) => (v.copy(contig="01"), va, gs)})

        val fileRoot = "/tmp/testGenWriter"
        val sampleFile = fileRoot + ".sample"
        val genFile = fileRoot + ".gen"
        val bgenFile = fileRoot + ".bgen"
        val qcToolLogFile = fileRoot + ".qctool.log"
        val qcToolPath = "qctool"

        hadoopDelete(bgenFile + ".idx", sc.hadoopConfiguration, true)
        hadoopDelete(genFile, sc.hadoopConfiguration, true)
        hadoopDelete(sampleFile, sc.hadoopConfiguration, true)
        hadoopDelete(qcToolLogFile, sc.hadoopConfiguration, true)

        var s = State(sc, sqlContext, vdsRemapped)
        s = SplitMulti.run(s, Array[String]())
        s = RenameSamples.run(s, Array("-i","/tmp/sample_rename.txt"))

        val origVds = s.vds

        GenWriter(fileRoot, s.vds, sc)

        s"src/test/resources/runExternalToolQuiet.sh $qcToolPath -force -g $genFile -s $sampleFile -og $bgenFile -log $qcToolLogFile" !

        if (vds.nVariants == 0)
          try {
            s = IndexBGEN.run(s, Array("-n", nPartitions.toString, bgenFile))
            s = ImportBGEN.run(s, Array("-s", sampleFile, "-n", nPartitions.toString, bgenFile))
            false
          } catch {
            case e:FatalException => true
            case _: Throwable => false
          }
        else {
          var q = IndexBGEN.run(State(sc, sqlContext, null), Array(bgenFile))
          q = ImportBGEN.run(State(sc, sqlContext, null), Array("-s", sampleFile, "-n", nPartitions.toString, bgenFile))
          val importedVds = q.vds

          assert(importedVds.nSamples == origVds.nSamples)
          assert(importedVds.nVariants == origVds.nVariants)
          assert(importedVds.sampleIds == origVds.sampleIds)

          val importedVariants = importedVds.variants
          val origVariants = origVds.variants

          val importedFull = importedVds.expandWithAll().map{case (v, va, s, sa, gt) => ((v, s), gt)}
          val originalFull = origVds.expandWithAll().map{case (v, va, s, sa, gt) => ((v, s), gt)}

          val result = originalFull.fullOuterJoin(importedFull).map{ case ((v, i), (gt1, gt2)) =>
            val gt1x = gt1 match {
              case Some(x) =>
                var newPl = x.pl.getOrElse(Array(0,0,0)).map{i => math.min(i, BgenLoader.MAX_PL)}
                val newGt = BgenUtils.parseGenotype(newPl)
                newPl = if (newGt == -1) null else newPl
                Some(x.copy(gt = Option(newGt), ad = None, dp = None, gq = None, pl = Option(newPl)))
              case None => None
            }
            if (gt1x == gt2)
              true
            else {
              if (gt1x.isDefined && gt2.isDefined)
                gt1x.get.pl.getOrElse(Array(BgenLoader.MAX_PL,BgenLoader.MAX_PL,BgenLoader.MAX_PL))
                  .zip(gt2.get.pl.getOrElse(Array(BgenLoader.MAX_PL,BgenLoader.MAX_PL,BgenLoader.MAX_PL)))
                  .forall { case (pl1, pl2) =>
                    if (math.abs(pl1 - pl2) <= 2) {
                      true
                    }
                      else {
                      println(s"$v $i $gt1x $gt2")
                      false
                    }
                  }
              else {
                println(s"$v $i $gt1x $gt2")
                false
              }
            }
          }.fold(true)(_ && _)

          result
        }
      }
  }

  @Test def testBgenImportRandom() {
      //Spec.check(300,100)
      Spec.check(30, 1000)
  }
}

object BgenUtils {
  def parseGenotype(pls: Array[Int]): Int = {
    if (pls(0) == 0 && pls(1) == 0
      || pls(0) == 0 && pls(2) == 0
      || pls(1) == 0 && pls(2) == 0)
      -1
    else {
      if (pls(0) == 0)
        0
      else if (pls(1) == 0)
        1
      else if (pls(2) == 0)
        2
      else
        -1
    }
  }
}