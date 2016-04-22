package org.broadinstitute.hail.driver

import org.apache.spark.HashPartitioner
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.driver.SingletonLDinTrios.ParentOfOrigin.ParentOfOrigin
import org.broadinstitute.hail.methods.{CompleteTrio, Filter, Pedigree}
import org.broadinstitute.hail.utils.SparseVariantSampleMatrix
import org.broadinstitute.hail.variant.GenotypeType.{GenotypeType => _, _}
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable
import scala.language.postfixOps

object SingletonLDinTrios extends Command {

  def name = "singletonLDinTrios"
  def description = "Looks at pairs of rare variants where both variants are both in at least one of the parents of one of the trios" +
    "and the other isn't. Compute how many are in/out of phase, stratified by the AC in ExAC (no binning here so filter wisely!)"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-i", aliases = Array("--controls"), usage = "Input ExAC .vds file")
    var controls_input: String = _

    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _

    @Args4jOption(required = true, name = "-g", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
    var gene_annotation: String = _

    @Args4jOption(required = true, name = "-c", aliases = Array("--consequence_annotation"), usage = "Annotation storing the gene information for aggregation")
    var consequence_annotation: String = _

    @Args4jOption(required = false, name = "-cexac", aliases = Array("--condition_AC"), usage = "Condition to apply to ExAC for filtering down the number of variants. Default is AC < 200")
    var cexac: String = "va.AC < 200"


  }
  def newOptions = new Options

  object ParentOfOrigin extends Enumeration {
    type ParentOfOrigin = Value
    val Mom = Value("0")
    val Dad = Value("1")
    val Both = Value("2")
  }

  /**
    * Records:
    * Number of variant pairs
    *
    **/
  class variantPairsCounter(val trios: SparseVariantSampleMatrix, val exac: SparseVariantSampleMatrix, val ped : Pedigree) {

    /** Stores the following:
      * (AC1,AC2) => (
      * #sites on the same trio haplotype,
      * #Sites on different trio haplotypes,
      * #Sites found co-seggregating in ExAC,
      * #Sites found in ExAC but not co-seggregating
      * #Sites on the same haplotype and found co-seggregating in ExAC
      * #Sites on different haplotype and not co-seggregating in ExAC, although present
      * */
    var res = mutable.Map[(Int, Int), (Int, Int, Int, Int, Int, Int)]()


    //Loop over the variants in the trios and gather the desired data:
    ped.completeTrios.foreach({ case (trio) =>
      //Get the list of unique pairs of het sites in the parents
      //val trioVariantPairs = for( (i,j) <- getUniqueVariantPairsInParents(trio) if(canBePhased(i,j,trio))) yield {
      // (exac.getAC(i), exac.getAC(j), isOnSameParentalHaplotype(i,j,trio),foundInSameSampleInExAC(i,j))
      //}

      //Compute and store results
      getUniqueVariantPairsInParents(trio).foreach({ case (v1, v2) =>
        //Only store results where sites could be trio-phased
        isOnSameParentalHaplotype(v1, v2, trio) match {
          case Some(sameTrioHap) =>
            val k = (exac.getAC(v1), exac.getAC(v2))
            //Check if could be found in ExAC
            foundInSameSampleInExAC(v1, v2) match {
              case Some(sameExacHap) =>
                val v = (
                  if (sameTrioHap) 1 else 0,
                  if (sameTrioHap) 0 else 1,
                  if (sameExacHap) 1 else 0,
                  if (sameExacHap) 0 else 1,
                  if (sameExacHap == sameTrioHap && sameTrioHap) 1 else 0,
                  if (sameExacHap == sameTrioHap && !sameTrioHap) 1 else 0
                  )
                addResult(k, v)
              case None =>
                val v = (
                  if (sameTrioHap) 1 else 0,
                  if (sameTrioHap) 0 else 1,
                  0, 0, 0, 0
                  )
                addResult(k, v)
            }
        }

      })
    })

    private def addResult(k: (Int, Int), v: (Int, Int, Int, Int, Int, Int)) = {
      res.get(k) match {
        case Some(pv) => res.update(k, (pv._1 + v._1, pv._2 + v._2, pv._3 + v._2, pv._4 + v._4, pv._5 + v._5, pv._6 + v._6))
        case None => res.update(k, v)
      }
    }

    private def getUniqueVariantPairsInParents(trio: CompleteTrio): Set[(String, String)] = {
      val momHetPairs = getHetVariantPairs(trios.getSample(trio.mom))
      val dadHetPairs = getHetVariantPairs(trios.getSample(trio.dad))
      //TODO Revisit this as there is probably a better solution
      (momHetPairs ++ dadHetPairs) &~ (momHetPairs & dadHetPairs)
    }

    private def getHetVariantPairs(sampleGenotypes: Option[Map[String, Genotype]]): Set[(String, String)] = {
      if (sampleGenotypes.isDefined) {
        val hetSites = for ((variant, gt) <- sampleGenotypes.get; if (gt.isHet)) yield variant
        return (for (i <- hetSites; j <- hetSites; if (i < j)) yield (i, j)).toSet
      }
      Set[(String, String)]()
    }

    //Returns parental origin when it can be inferred from the trio, or None if it cannot
    private def parentalOrigin(kidGT: Option[Genotype], dadGT: Option[Genotype], momGT: Option[Genotype]): Option[ParentOfOrigin] = {
      if (kidGT.isDefined && dadGT.isDefined && momGT.isDefined && kidGT.get.isHet) {
        if (dadGT.get.isHomRef && (momGT.get.isHet || momGT.get.isHomVar)) {
          return Some(ParentOfOrigin.Mom)
        }
        else if (momGT.get.isHomRef && (dadGT.get.isHet || dadGT.get.isHomVar)) {
          return Some(ParentOfOrigin.Dad)
        }
        else if (dadGT.get.isHet && momGT.get.isHomVar) {
          return Some(ParentOfOrigin.Mom)
        }
        else if (momGT.get.isHet && dadGT.get.isHomVar) {
          return Some(ParentOfOrigin.Dad)
        }
      }
      None
    }

    private def isOnSameParentalHaplotype(variantID1: String, variantID2: String, trio: CompleteTrio): Option[Boolean] = {

      val v1POO = parentalOrigin(trios.getGenotype(variantID1, trio.kid), trios.getGenotype(variantID1, trio.dad), trios.getGenotype(variantID1, trio.mom))
      val v2POO = parentalOrigin(trios.getGenotype(variantID2, trio.kid), trios.getGenotype(variantID2, trio.dad), trios.getGenotype(variantID2, trio.mom))

      (v1POO, v2POO) match {
        case (Some(v1poo), Some(v2poo)) => Some(v1poo == v2poo)
        case _ => None
      }

    }

    private def foundInSameSampleInExAC(variantID1: String, variantID2: String, minSamples: Int = 1): Option[Boolean] = {

      (exac.variants.get(variantID1), exac.variants.get(variantID2)) match {
        case (Some(v1), Some(v2)) => Some((exac.variants(variantID1).filter({ case (k, v) => v == GenotypeType.Het || v == GenotypeType.HomVar }).keySet.intersect(
          exac.variants(variantID2).filter({ case (k, v) => v == GenotypeType.Het || v == GenotypeType.HomVar }).keySet).size) >= minSamples)
        case _ => None
      }

    }
  }


  def run(state: State, options: Options): State = {
    //Read PED file
    val ped = Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds)

    //List of contigs to consider
    val autosomes = Range(1,23).map({c: Int => c.toString})
    def autosomeFilter = {(v: Variant, va: Annotation) => autosomes.contains(v.contig)}

    //List individuals from trios where all family members are present
    //TODO: Check that completeTrios actually lists all complete trios in the fam file.
    val samplesInTrios = ped.completeTrios.foldLeft(List[String]())({case (acc,trio) => trio.mom::trio.dad::trio.kid::acc})
    //Filter trios and keep only complete trios
    val trioVDS = state.vds.filterSamples((s: String, sa: Annotation) => Filter.keepThis(samplesInTrios.contains(s), true)).filterVariants(autosomeFilter)

    //Load ExAC VDS, filter common samples and sites based on exac condition (AC)
    val exacVDS = FilterVariants.run(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),Array("-c",options.cexac)).vds.
      filterSamples((s: String, sa: Annotation) => Filter.keepThis(trioVDS.sampleIds.contains(s), false)).filterVariants(autosomeFilter)

    //Get annotations
    val triosGeneAnn = trioVDS.queryVA(options.gene_annotation)._2
    //val triosConsAnn = trioVDS.queryVA(options.consequence_annotation)._2
    val exacGeneAnn = exacVDS.queryVA(options.gene_annotation)._2

    val partitioner = new HashPartitioner(200)

    val triosRDD = trioVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(trioVDS.sampleIds))({
      case(counter,v,va,s,sa,g) =>
        counter.addVariantGenotype(s,v.toString(),g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (va) => triosGeneAnn(va)}
    )

    val exacRDD = exacVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(exacVDS.sampleIds))({
      case(counter,v,va,s,sa,g) =>
        counter.addVariantGenotype(s,v.toString(),g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (va) => exacGeneAnn(va)}
    )

    val callsByGene = triosRDD.join(exacRDD)

    callsByGene.map({case(gene,(trios,exac)) => (gene,new variantPairsCounter(trios,exac,ped))})

    state
  }
}
