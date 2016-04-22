package org.broadinstitute.hail.driver

import org.apache.spark.HashPartitioner
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.driver.SingletonLDinTrios.ParentOfOrigin.ParentOfOrigin
import org.broadinstitute.hail.methods.{CompleteTrio, Filter, MendelErrors, Pedigree}
import org.broadinstitute.hail.utils.SparseVariantSampleMatrix
import org.broadinstitute.hail.variant.GenotypeType.{GenotypeType => _, _}
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.mutable
import scala.language.postfixOps
import scala.sys.process._

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
  class variantPairsCounter(val trios: SparseVariantSampleMatrix, val exac: SparseVariantSampleMatrix, val ped : Pedigree){


    //Loop over the variants in the trios and gather the following data:

    var AC1 = Array[Int]()
    var AC2 = Array[Int]()
    var sameHap = Array[Boolean]()
    var correctPhase = Array[Boolean]()


    for(trio <- ped.completeTrios ){

      //Check if either of the parents has two het sites
      val dad = trios.getSample(trio.dad)
      if(dad.isDefined){
        val dadHets = for((variant,gt) <- dad.get; if(gt.isHet)) yield variant
        if(dadHets.size>1){

          for( i<-dadHets; j <- dadHets; if(i < j && canBePhased(i,j,trio))){
            //Get trio-phasing and ExAC "Phasing"
            val trioPhase = isOnSameParentalHaplotype(i,j,trio)
            val exacACi = if(exac.variants.isDefinedAt(i)) exac.variants(i).foldLeft(0)({case(acc,(s,gt)) => 0}) else 0

          }

        }
      }



    }

    //Returns parental origin when it can be inferred from the trio, or None if it cannot
    private def parentalOrigin(kidGT: Option[Genotype], dadGT: Option[Genotype], momGT: Option[Genotype]): Option[ParentOfOrigin] ={
        if(kidGT.isDefined && dadGT.isDefined && momGT.isDefined && kidGT.get.isHet){
          if(dadGT.get.isHomRef && (momGT.get.isHet || momGT.get.isHomVar)) { return Some(ParentOfOrigin.Mom)}
          else if(momGT.get.isHomRef && (dadGT.get.isHet || dadGT.get.isHomVar)){ return Some(ParentOfOrigin.Dad)}
          else if(dadGT.get.isHet && momGT.get.isHomVar){return Some(ParentOfOrigin.Mom)}
          else if(momGT.get.isHet && dadGT.get.isHomVar){return Some(ParentOfOrigin.Dad)}
        }
        None
    }

    private def isOnSameParentalHaplotype(variantID1: String, variantID2: String, trio: CompleteTrio) : Option[Boolean] = {

      val v1POO = parentalOrigin(trios.getGenotype(variantID1,trio.kid),trios.getGenotype(variantID1,trio.dad),trios.getGenotype(variantID1,trio.mom))
      val v2POO = parentalOrigin(trios.getGenotype(variantID2,trio.kid),trios.getGenotype(variantID2,trio.dad),trios.getGenotype(variantID2,trio.mom))

      (v1POO, v2POO) match {
        case (Some(v1poo), Some (v2poo)) => Some(v1poo == v2poo)
        case _ => None
      }

    }

    private def canBePhased(variantID1: String, variantID2: String, trio: CompleteTrio) : Boolean = {

      val dadGT = trios.getGenotype(variantID1,trio.dad)
      if(!dadGT.isDefined || dadGT.get.isNotCalled){return false}

      val momGT = trios.getGenotype(variantID1,trio.mom)
      if(!momGT.isDefined || momGT.get.isNotCalled){return false}

      val kidGT = trios.getGenotype(variantID1,trio.kid)
      if(!kidGT.isDefined || kidGT.get.isNotCalled){return false}

      !(dadGT.get.isHet && momGT.get.isHet && kidGT.get.isHet)
    }

    private def foundInSameSampleInExAC(variantID1: String, variantID2: String, minSamples: Int = 1): Boolean ={
      (exac.variants(variantID1).filter({case (k,v) => v == GenotypeType.Het || v == GenotypeType.HomVar}).keySet.intersect(
        exac.variants(variantID2).filter({case (k,v) => v == GenotypeType.Het || v == GenotypeType.HomVar}).keySet).size) >= minSamples
    }

    private def isCompoundHet(variantIDs: List[String], phasingData: SparseVariantSampleMatrix, minPhasingSamples: Int = 1) : Boolean = {

      val phasingVariants = phasingData.variants

      //Loop over all pairs of variants (ordered lexicographically)
      (for( i<-variantIDs; j <- variantIDs; if(i < j && phasingVariants.contains(i) && phasingVariants.contains(j))) yield{
        //For each pair of variants, count how many samples carry both
        (phasingVariants(i).filter({case (k,v) => v == GenotypeType.Het || v == GenotypeType.HomVar}).keySet.intersect(
          phasingVariants(j).filter({case (k,v) => v == GenotypeType.Het || v == GenotypeType.HomVar}).keySet).size)

      }).max > minPhasingSamples
    }


  }


  def run(state: State, options: Options): State = {
    val ped = Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds)

    //List individuals from trios where all family members are present
    //TODO: Check that completeTrios actually lists all complete trios in the fam file.
    val samplesInTrios = ped.completeTrios.foldLeft(List[String]())({case (acc,trio) => trio.mom::trio.dad::trio.kid::acc})
    //Filter trios and keep only complete trios
    val trioVDS = state.vds.filterSamples((s: String, sa: Annotation) => Filter.keepThis(samplesInTrios.contains(s), true)))

    //Load ExAC VDS, filter common samples and sites based on exac condition (AC)
    val exacVDS = FilterVariants.run(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),Array("-c",options.cexac)).vds.
      filterSamples((s: String, sa: Annotation) => Filter.keepThis(trioVDS.sampleIds.contains(s), false))

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



    state
  }
}
