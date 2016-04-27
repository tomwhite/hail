package org.broadinstitute.hail.driver

import org.apache.spark.HashPartitioner
import org.broadinstitute.hail.{Logging, RichRDD}
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.methods.{Filter, Pedigree}
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

    //@Args4jOption(required = true, name = "-c", aliases = Array("--consequence_annotation"), usage = "Annotation storing the gene information for aggregation")
    //var consequence_annotation: String = _

    //@Args4jOption(required = false, name = "-cexac", aliases = Array("--condition_AC"), usage = "Condition to apply to ExAC for filtering down the number of variants. Default is AC < 200")
    //var cexac: String = "va.info.AC[0] < 200"

    @Args4jOption(required = true, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
    var number_partitions: Int = 200


  }
  def newOptions = new Options

  /**
    * Records:
    * Number of variant pairs
    *
    **/

   object VariantPairsCounter{
    def getHeaderString() : String = {
      "AC1\tAC2\tsameTrioHap\tdiffTrioHap\tcoInExAC\tnotCoInExAC\tsameHapTrioAndExAC\tdiffHapTrioAndExAC"
    }
  }

  class VariantPairsCounter(val trios: SparseVariantSampleMatrix, val ped : Pedigree) extends Logging{

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

    val variantPairs = (for(trio <- ped.completeTrios) yield{
      getHetPhasedVariantPairs(trio.kid,trio.dad,trio.mom) ::: getHetPhasedVariantPairs(trio.kid,trio.mom,trio.dad)
    }).flatten



    //Public functions
    override def toString() : String = {
      res.map({case ((ac1,ac2),(sameTrioHap,diffTrioHap,coInExAC,notCoInExAC,sameHapTrioExAC,diffHapTrioExAC)) =>
      ("%d\t" * 8).format(ac1,ac2,sameTrioHap,diffTrioHap,coInExAC,notCoInExAC,sameHapTrioExAC,diffHapTrioExAC)}).mkString("\r")
    }

    def getHetSites() : Set[String] = {
      variantPairs.flatMap({case (v1,v2,phase) => List(v1,v2)}).toSet
    }

    def this(trios: SparseVariantSampleMatrix, exac : SparseVariantSampleMatrix , ped : Pedigree) = {
      this(trios,ped)
      computeExACphase(exac)
    }


    //Private functions
    private def computeExACphase(exac : SparseVariantSampleMatrix) = {
      variantPairs.foreach({ case (v1, v2, sameTrioHap) =>
        //Only store results where sites could be trio-phased
        if (sameTrioHap.isDefined) {
          val k = (exac.getAC(v1), exac.getAC(v2))
          //Check if could be found in ExAC
          foundInSameSampleInExAC(exac,v1, v2) match {
            case Some(sameExacHap) =>
              val v = (
                if (sameTrioHap.get) 1 else 0,
                if (sameTrioHap.get) 0 else 1,
                if (sameExacHap) 1 else 0,
                if (sameExacHap) 0 else 1,
                if (sameExacHap == sameTrioHap.get && sameTrioHap.get) 1 else 0,
                if (sameExacHap == sameTrioHap.get && !sameTrioHap.get) 1 else 0
                )
              addResult(k, v)
            case None =>
              val v = (
                if (sameTrioHap.get) 1 else 0,
                if (sameTrioHap.get) 0 else 1,
                0, 0, 0, 0
                )
              addResult(k, v)
          }
        }
      })
    }

    private def addResult(k: (Int, Int), v: (Int, Int, Int, Int, Int, Int)) = {
      res.get(k) match {
        case Some(pv) => res.update(k, (pv._1 + v._1, pv._2 + v._2, pv._3 + v._3, pv._4 + v._4, pv._5 + v._5, pv._6 + v._6))
        case None => res.update(k, v)
      }
    }

    //Returns all pairs of variants for which the parent parentID is het at both sites, along with
    //whether each of the variant pairs are on the same haplotype or not based on the transmission of alleles in the trio (or None if ambiguous / missing data)
    private def getHetPhasedVariantPairs(kidID: String, parentID: String, otherParentID: String) : List[(String,String,Option[Boolean])] = {
      (trios.getSample(parentID) match {
        case Some(genotypes) => {
          for((variant1, gt1) <- genotypes; (variant2,gt2) <- genotypes; if(gt1.isHet && gt2.isHet && variant1 < variant2)) yield{
            (variant1, variant2, isOnSameParentalHaplotype(variant1,variant2,kidID,otherParentID))
          }
        }.toList
        case None => List.empty
      })
    }

    //Given a site that is Het in parent1,
    //returns whether the site was transmitted from parent1 or from the otherParent
    //Returns None if ambiguous or missing data
    private def isHetSiteTransmitted(kidGT: Option[Genotype], otherParentGT: Option[Genotype]): Option[Boolean] = {
      kidGT match {
        case Some(kid) => {
          if(kid.isHomRef){ return Some(false)}
          else if(kid.isHomVar){return Some(true)}
          else if(kid.isHet){
            otherParentGT match {
              case Some(gt) => {
                if(gt.isHomRef){ return Some(true)}
                else if(gt.isHomVar){return Some(false)}
              }
              case None => None
            }
          }
        }
        case None => None
      }
      None
    }


    //Given two variants that are het in a parent, computes whether they are on the same haplotype or not based on
    //the child and the otherParent in the trio.
    //Returns None if ambiguous or missing datta
    private def isOnSameParentalHaplotype(variantID1: String, variantID2: String, kidID: String, otherParentID: String): Option[Boolean] = {

      val v1POO = isHetSiteTransmitted(trios.getGenotype(variantID1, kidID), trios.getGenotype(variantID1, otherParentID))
      val v2POO = isHetSiteTransmitted(trios.getGenotype(variantID2, kidID), trios.getGenotype(variantID2, otherParentID))

      (v1POO, v2POO) match {
        case (Some(v1poo), Some(v2poo)) => Some(v1poo == v2poo)
        case _ => None
      }

    }

    //Given two variants, check if any sample in ExAC carries both of these variants. Then compares the number of samples carrying
    //both variants to the minSamples.
    private def foundInSameSampleInExAC(exac: SparseVariantSampleMatrix, variantID1: String, variantID2: String, minSamples: Int = 1): Option[Boolean] = {

      (exac.variants.get(variantID1), exac.variants.get(variantID2)) match {
        case (Some(v1), Some(v2)) => Some((v1.filter({
          case (k, v) => v.isHet || v.isHomVar
        }).keySet.intersect(
          v2.filter({
            case (k, v) => v.isHet || v.isHomVar
          }).keySet).size) >= minSamples)
        case _ => None
      }

    }
  }


  def run(state: State, options: Options): State = {
    //Read PED file
    val ped = state.sc.broadcast(Pedigree.read(options.famFilename, state.hadoopConf, state.vds.sampleIds))

    //List of contigs to consider
    val autosomes = Range(1,23).map({c: Int => c.toString})
    def autosomeFilter = {(v: Variant, va: Annotation) => autosomes.contains(v.contig)}

    //List individuals from trios where all family members are present
    //TODO: Check that completeTrios actually lists all complete trios in the fam file.
    val samplesInTrios = ped.value.completeTrios.foldLeft(List[String]())({case (acc,trio) => trio.mom::trio.dad::trio.kid::acc})
    //Filter trios and keep only complete trios
    val trioVDS = state.vds.filterSamples((s: String, sa: Annotation) => Filter.keepThis(samplesInTrios.contains(s), true)).filterVariants(autosomeFilter)


    //Get annotations
    val triosGeneAnn = trioVDS.queryVA(options.gene_annotation)._2
    //val triosConsAnn = trioVDS.queryVA(options.consequence_annotation)._2

    val partitioner = new HashPartitioner(options.number_partitions)

    val triosRDD = trioVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(trioVDS.sampleIds))({
      case(counter,v,va,s,sa,g) =>
        counter.addVariantGenotype(v.toString(),s,g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (v,va) => triosGeneAnn(va)}
    )

    //Get unique variants that are found in pairs in our samples
    val uniqueVariants = triosRDD.map({
      case(gene,svm) => new VariantPairsCounter(svm,ped.value).getHetSites()}).reduce(
      {case(v1,v2) => v1 ++ v2}
    )

    val bcUniqueVariants = state.sc.broadcast(uniqueVariants)

    def variantsOfInterestFilter = {(v: Variant, va: Annotation) => bcUniqueVariants.value.contains(v.toString)}
    
    //Load ExAC VDS, filter common samples and sites based on exac condition (AC)
    //val exacVDS = FilterVariants.run(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),Array("-c",options.cexac,"--keep")).vds.
    //  filterSamples((s: String, sa: Annotation) => Filter.keepThis(trioVDS.sampleIds.contains(s), false)).filterVariants(autosomeFilter)

    val exacVDS = State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)).vds.
      filterSamples((s: String, sa: Annotation) => Filter.keepThis(trioVDS.sampleIds.contains(s), false)).filterVariants(variantsOfInterestFilter)

    val exacGeneAnn = exacVDS.queryVA(options.gene_annotation)._2

    val exacRDD = exacVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(exacVDS.sampleIds))({
      case(counter,v,va,s,sa,g) =>
        counter.addVariantGenotype(v.toString(),s,g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (v,va) => exacGeneAnn(va)}
    )

    val callsByGene = triosRDD.join(exacRDD)

    //write results
    new RichRDD(callsByGene.map(
      {case(gene,(trios,exac)) =>
        val gString = gene match{
          case Some(gene) => gene
          case None => "NA"
        }
        gString + "\t" + (new VariantPairsCounter(trios,exac,ped.value)).toString()
      })).writeTable(options.output,header = Some("gene\t" + VariantPairsCounter.getHeaderString()))

    state
  }
}
