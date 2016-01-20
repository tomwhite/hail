package org.broadinstitute.hail.driver

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.variant.Variant
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.methods.ConcordanceTable
import org.kohsuke.args4j.{Option => Args4jOption}

object Concordance extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-o", aliases = Array("--output"), usage = "Output file root name")
    var output: String = _
    @Args4jOption(required = false, name = "-s", aliases = Array("--store"),
      usage = "Store concordance output in vds annotations", forbids = Array("output"))
    var store: Boolean = false
  }

  def newOptions = new Options

  def name = "concordance"

  def description = "Compare two vds with overlapping samples and variants"

  val signatures = Map(
    "HomRef_HomRef" -> new SimpleSignature("Long", "toLong"),
    "HomRef_Het" -> new SimpleSignature("Long", "toLong"),
    "HomRef_HomAlt" -> new SimpleSignature("Long", "toLong"),
    "HomRef_NoCall" -> new SimpleSignature("Long", "toLong"),
    "HomRef_None" -> new SimpleSignature("Long", "toLong"),

    "Het_HomRef" -> new SimpleSignature("Long", "toLong"),
    "Het_Het" -> new SimpleSignature("Long", "toLong"),
    "Het_HomAlt" -> new SimpleSignature("Long", "toLong"),
    "Het_NoCall" -> new SimpleSignature("Long", "toLong"),
    "Het_None" -> new SimpleSignature("Long", "toLong"),

    "HomAlt_HomRef" -> new SimpleSignature("Long", "toLong"),
    "HomAlt_Het" -> new SimpleSignature("Long", "toLong"),
    "HomAlt_HomAlt" -> new SimpleSignature("Long", "toLong"),
    "HomAlt_NoCall" -> new SimpleSignature("Long", "toLong"),
    "HomAlt_None" -> new SimpleSignature("Long", "toLong"),

    "NoCall_HomRef" -> new SimpleSignature("Long", "toLong"),
    "NoCall_Het" -> new SimpleSignature("Long", "toLong"),
    "NoCall_HomAlt" -> new SimpleSignature("Long", "toLong"),
    "NoCall_NoCall" -> new SimpleSignature("Long", "toLong"),
    "NoCall_None" -> new SimpleSignature("Long", "toLong"),

    "None_HomRef" -> new SimpleSignature("Long", "toLong"),
    "None_Het" -> new SimpleSignature("Long", "toLong"),
    "None_HomAlt" -> new SimpleSignature("Long", "toLong"),
    "None_NoCall" -> new SimpleSignature("Long", "toLong"),
    "None_None" -> new SimpleSignature("Long", "toLong"),

    "concrate" -> new SimpleSignature("Double", "toDouble"),
    "n" -> new SimpleSignature("Long", "toLong"))

  def run(state: State, options: Options): State = {
    require(state.vds2.isDefined)

    val vds1 = state.vds
    val vds2 = state.vds2.get
    val mergedVSM = vds1.joinOuterOuter(vds2)
    mergedVSM.cache()

    def variantString(v: Variant): String = v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt

    def sampleConcordance: RDD[(Int, ConcordanceTable)] = {
      mergedVSM
        .aggregateBySample[ConcordanceTable](new ConcordanceTable)((comb, gtp) => comb.addCount(gtp._1, gtp._2),
        (comb1, comb2) => comb1.merge(comb2))
    }

    def variantConcordance: RDD[(Variant, ConcordanceTable)] = {
      mergedVSM
        .aggregateByVariant[ConcordanceTable](new ConcordanceTable)((comb, gtp) => comb.addCount(gtp._1, gtp._2),
        (comb1, comb2) => comb1.merge(comb2))
    }

    def genotypeCombinations(sep: String = "\t"): String = {
      val possibleTypes: Array[Option[GenotypeType]] = Array(Some(HomRef), Some(Het), Some(HomVar), Some(NoCall), None)
      val typeNames = Map(Some(HomRef) -> "HomRef", Some(Het) -> "Het", Some(HomVar) -> "HomVar", Some(NoCall) -> "NoCall", None -> "None")
      val gcomb = for (i <- possibleTypes; j <- possibleTypes) yield typeNames.get(i).get + ":" + typeNames.get(j).get
      gcomb.mkString(sep)
    }

    def writeSampleConcordance(filename: String, sep: String = "\t") {
      val sampleIds = mergedVSM.sampleIds
      val gcomb = genotypeCombinations(sep)
      val header = s"ID${sep}nVar${sep}Concordance${sep}$gcomb\n"

      sampleConcordance
        .map { case (s, ct) =>
          val sb = new StringBuilder()
          sb.append(sampleIds(s))
          sb.append(sep)
          sb.append(ct.numCalled)
          sb.append(sep)
          sb.append(ct.concordanceRate.map(x => "%.2f".format(x * 100)).getOrElse("NaN"))
          sb.append(sep)
          sb.append(ct.counts.mkString(sep))
          sb.append("\n")
          sb.result()
        }
        .writeTable(filename, Some(header), deleteTmpFiles = true)
    }

    def writeVariantConcordance(filename: String, sep: String = "\t"): Unit = {
      val gcomb = genotypeCombinations(sep)
      val header = s"Variant${sep}nSamples${sep}Concordance${sep}$gcomb\n"

      variantConcordance
        .map { case (v, ct) =>
          val sb = new StringBuilder()
          sb.append(variantString(v))
          sb.append(sep)
          sb.append(ct.numCalled)
          sb.append(sep)
          sb.append(ct.concordanceRate.map(x => "%.2f".format(x * 100)).getOrElse("NaN"))
          sb.append(sep)
          sb.append(ct.counts.mkString(sep))
          sb.append("\n")
          sb.result()
        }
        .writeTable(filename, Some(header), deleteTmpFiles = true)
    }

    val overallCT = sampleConcordance.fold(0, new ConcordanceTable)((a, b) => (a._1 + b._1, a._2.merge(b._2)))._2
    println("nSamples: " + mergedVSM.nLocalSamples)
    println("nVariants: " + mergedVSM.nVariants)
    val percOverlap = divOption(overallCT.numCalled, overallCT.numTotal).map(x => "%.2f".format(x * 100)).getOrElse("NaN")
    println("nGenotypesTested: " + overallCT.numCalled + " (out of " + mergedVSM.nLocalSamples * mergedVSM.nVariants + ")")
    println("concordance (%): " + overallCT.concordanceRate.map(x => "%.2f".format(x * 100)).getOrElse("NaN"))


    if (options.store) {
      val indexMapping = for (s <- vds1.sampleIds) yield mergedVSM.localSamples.indexOf(mergedVSM.sampleIds.indexOf(s))
      val sampleConcMap = sampleConcordance.collectAsMap()
      val varConcMap = variantConcordance.collectAsMap()

      val newSampleAnnotations = vds1.metadata.sampleAnnotations
        .zipWithIndex
        .map { case (sa, s) => sa.addMap("conc", sampleConcMap.get(indexMapping(s)) match {
          case Some(x) => x.results
          case None => Map.empty[String, String]
        })
        }

      state.copy(
        vds = vds1.copy(
          metadata = vds1.metadata.copy(
            sampleAnnotationSignatures = vds1.metadata.sampleAnnotationSignatures
              .addMap("conc", signatures),
            sampleAnnotations = vds1.metadata.sampleAnnotations
              .zip(newSampleAnnotations)
              .map { case (oldAnno, newAnno) => oldAnno ++ newAnno },
            variantAnnotationSignatures = vds1.metadata.variantAnnotationSignatures
              .addMap("conc", signatures)
          ),
          rdd = vds1.rdd.map { case (v, va, gs) => (v, va.addMap("conc", varConcMap(v).results), gs) }
        )
      )
    } else {
      writeSampleConcordance(options.output + ".sconc.txt", "\t")
      writeVariantConcordance(options.output + ".vconc.txt", "\t")
      state
    }
  }
}
