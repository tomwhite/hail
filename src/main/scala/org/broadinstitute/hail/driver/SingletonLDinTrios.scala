package org.broadinstitute.hail.driver

import java.io.{BufferedWriter, File, FileWriter}

import breeze.linalg.{DenseVector, SparseVector, max, sum}
import breeze.numerics.abs
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.broadinstitute.hail.{Logging, RichRDD}
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.methods.{Filter, Pedigree}
import org.broadinstitute.hail.utils.SparseVariantSampleMatrix
import org.broadinstitute.hail.variant.GenotypeType.{GenotypeType => _, _}
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.Utils._

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

    @Args4jOption(required = false, name = "-p", aliases = Array("--partitions_number"), usage = "Number of partitions to use for gene aggregation.")
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
      "AC1\tAC2\tsameTrioHap\tdiffTrioHap\tcoInExAC\tnotCoInExAC\tsameHapTrioAndExAC\tdiffHapTrioAndExAC\tsameHapExACEM\tdiffHapExACEM\tsameHapTrioAndExACEM\tdiffHapTrioAndExACEM"
    }
  }

  class VariantPairsCounter(val trios: SparseVariantSampleMatrix, val ped : Pedigree) extends Logging{

    /** Stores the following results:
      * #sites on the same trio haplotype,
      * #Sites on different trio haplotypes,
      * #Sites found co-seggregating in ExAC,
      * #Sites found in ExAC but not co-seggregating
      * #Sites on the same haplotype and found co-seggregating in ExAC
      * #Sites on different haplotype and not co-seggregating in ExAC, although present
      * #Sites on the same haplotype in ExAC based on EM
      * #Sites on different haplotype in ExAC based on EM
      * #Sites on the same haplotype in ExAC based on EM and on the same trio haplotype
      * #Sites on different haplotype in ExAC based on EM and on different trio haplotype
      * */
    class VPCResult {

      //Numbers from trio inheritance
      var nSameTrioHap = 0
      var nDiffTrioHap = 0

      //Numbers from naive found/not found in ExAC
      var nCoSegExAC = 0
      var nNonCoSegExac = 0
      var nCoSegExACandSameTrioHap = 0
      var nNonCoSegExACandDiffTrioHap = 0

      //Numbers from EM
      var nSameHapExAC = 0
      var nDiffHapExac = 0
      var nSameHapExACandSameHapTrio = 0
      var nDiffHapExACandDiffHapTrio = 0

      def this(sameTrioHap : Boolean){
        this()
        nSameTrioHap = if(sameTrioHap) 1 else 0
        nDiffTrioHap = if(sameTrioHap) 0 else 1
      }

      def coSegExAC(coseg : Boolean, sameTrioHap : Boolean) ={
        if(coseg){
          nCoSegExAC += 1
          if(sameTrioHap){
            nCoSegExACandSameTrioHap += 1
          }
        }else{
          nNonCoSegExac +=1
          if(!sameTrioHap){
            nNonCoSegExACandDiffTrioHap += 1
          }
        }
      }

      def sameHapExAC(sameHap : Boolean, sameTrioHap : Boolean) ={
        if(sameHap){
          nSameHapExAC += 1
          if(sameTrioHap){
            nSameHapExACandSameHapTrio += 1
          }
        }else{
          nDiffHapExac +=1
          if(!sameTrioHap){
            nDiffHapExACandDiffHapTrio += 1
          }
        }
      }

      def add(that: VPCResult): VPCResult ={
         nSameTrioHap += that.nSameTrioHap
         nDiffTrioHap += that.nDiffTrioHap

        //Numbers from naive += that.naive found/not found in ExAC
         nCoSegExAC += that.nCoSegExAC
         nNonCoSegExac += that.nNonCoSegExac
         nCoSegExACandSameTrioHap += that.nCoSegExACandSameTrioHap
         nNonCoSegExACandDiffTrioHap += that.nNonCoSegExACandDiffTrioHap

        //Numbers from EM
         nSameHapExAC += that.nSameHapExAC
         nDiffHapExac += that.nDiffHapExac
         nSameHapExACandSameHapTrio += that.nSameHapExACandSameHapTrio
         nDiffHapExACandDiffHapTrio += that.nDiffHapExACandDiffHapTrio
        this
      }

      override def toString() : String = {
        (("%d\t" * 9) + "%d").format(
         nSameTrioHap,
         nDiffTrioHap,
         nCoSegExAC,
         nNonCoSegExac,
         nCoSegExACandSameTrioHap,
         nNonCoSegExACandDiffTrioHap,
         nSameHapExAC,
         nDiffHapExac,
         nSameHapExACandSameHapTrio,
         nDiffHapExACandDiffHapTrio
        )
      }

    }

    var res = mutable.Map[(Int, Int), VPCResult]()

    val variantPairs = (for(trio <- ped.completeTrios) yield{
      getHetPhasedVariantPairs(trio.kid,trio.dad,trio.mom) ::: getHetPhasedVariantPairs(trio.kid,trio.mom,trio.dad)
    }).flatten



    //Public functions
    override def toString() : String = {
      res.map({case ((ac1,ac2),result) =>
      ("%d\t%d\t").format(ac1,ac2) + result.toString()}).mkString("\r")
    }

    def getHetSites() : Set[String] = {
      variantPairs.flatMap({case (v1,v2,phase) => List(v1,v2)}).toSet
    }

    def this(trios: SparseVariantSampleMatrix, exac : SparseVariantSampleMatrix , ped : Pedigree) = {
      this(trios,ped)
      computeExACphase(exac)
    }

    def addExac(exac: SparseVariantSampleMatrix) = {
      computeExACphase(exac)
    }

    //Private functions
    private def computeExACphase(exac : SparseVariantSampleMatrix) = {
      info("Computing ExAC phase for "+variantPairs.size+" variant pairs...")
      variantPairs.foreach({ case (v1, v2, sameTrioHap) =>
        info("Starting variant-pair:" + v1 +" | "+v2)
        //Only store results where sites could be trio-phased
        if (sameTrioHap.isDefined) {
          val k = (exac.getAC(v1), exac.getAC(v2))
          val v = new VPCResult(sameTrioHap.get)
          //Check if could be found in ExAC and how it seggregates
          foundInSameSampleInExAC(exac,v1, v2) match {
            case Some(sameExacHap) =>
              v.coSegExAC(sameExacHap,sameTrioHap.get)
            case None =>
          }

          //Compute whether on the same haplotype based on EM using ExAC
          probOnSameHaplotypeWithEM(exac,v1,v2) match {
            case Some(probSameHap) => v.sameHapExAC(probSameHap>0.5,sameTrioHap.get)
            case None =>
          }

          //Add results
          res.get(k) match{
            case Some(pv) => res.update(k, pv.add(v))
            case None => res.update(k,v)
          }

        }
      })
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

      (exac.getVariant(variantID1), exac.getVariant(variantID2)) match {
        case (Some(v1), Some(v2)) => Some((v1.filter({
          case (k, v) => v.isHet || v.isHomVar
        }).keySet.intersect(
          v2.filter({
            case (k, v) => v.isHet || v.isHomVar
          }).keySet).size) >= minSamples)
        case _ => None
      }

    }

    private def probOnSameHaplotypeWithEM(exac: SparseVariantSampleMatrix, variantID1: String, variantID2: String) : Option[Double] = {
      phaseWithEM(exac,variantID1,variantID2) match{
        case Some(haplotypes) =>
          return Some(haplotypes(1) * haplotypes(2) / (haplotypes(1) * haplotypes(2) + haplotypes(0) * haplotypes(3)))
        case None => None
      }

    }

    /**Returns a vector containing the estimated number of haplotypes
      * (0) AB
      * (1) Ab
      * (2) aB
      * (3) ab
      */
    private def phaseWithEM(exac: SparseVariantSampleMatrix, variantID1: String, variantID2: String) : Option[DenseVector[Double]] = {

      /**Count the number of individuals with different genotype combinations
        * (0) AABB
        * (1) AaBB
        * (2) aaBB
        * (3) AABb
        * (4) AaBb
        * (5) aaBb
        * (6) AAbb
        * (7) Aabb
        * (8) aabb
        */
      val gtCounts = new DenseVector(exac.sampleIDs.foldLeft(new Array[Int](9))({case (acc,s) =>

        (exac.getGenotype(variantID1,s) ,exac.getGenotype(variantID2,s)) match{
          case (Some(gt1),Some(gt2)) => {
            if(gt1.isHomRef){
              if(gt2.isHomRef){acc(0)+=1}
              else if(gt2.isHet){acc(3)+=1}
              else if(gt2.isHomVar){acc(6)+=1}
            }
            else if(gt1.isHet){
              if(gt2.isHomRef){acc(1)+=1}
              else if(gt2.isHet){acc(4)+=1}
              else if(gt2.isHomVar){acc(7)+=1}
            }
            else if(gt1.isHomVar){
              if(gt2.isHomRef){acc(2)+=1}
              else if(gt2.isHet){acc(5)+=1}
              else if(gt2.isHomVar){acc(8)+=1}
            }
          }
          case _ =>
        }
        acc
      }))

      val nSamples = gtCounts.foldLeft(0)({case (acc,x) => acc+x})

      //Needs some non-ref samples to compute
      if(gtCounts(0) >= nSamples){ return None}

      val nHaplotypes = 2.0*nSamples.toDouble

      /**
        * Constant quantities for each of the different haplotypes:
        * n.AB => 2*n.AABB + n.AaBB + n.AABb
        * n.Ab => 2*n.AAbb + n.Aabb + n.AABb
        * n.aB => 2*n.aaBB + n.AaBB + n.aaBb
        * n.ab => 2*n.aabb + n.aaBb + n.Aabb
        */
      val const_counts = new DenseVector(Array[Double](
        2.0*gtCounts(0) + gtCounts(1) + gtCounts(3), //n.AB
        2.0*gtCounts(6) + gtCounts(3) + gtCounts(7), //n.Ab
        2.0*gtCounts(2) + gtCounts(1) + gtCounts(5), //n.aB
        2.0*gtCounts(8) + gtCounts(5) + gtCounts(7)  //n.ab
      ))

      //Initial estimate with AaBb contributing equally to each haplotype
      var p_next = (const_counts :+ new DenseVector(Array.fill[Double](4)(gtCounts(4)/2.0))) :/ nHaplotypes
      var p_cur = p_next :+ 1.0

      //EM
      while(max(abs(p_next :- p_cur)) > 1e-7){

        p_cur = p_next

        p_next = (const_counts :+
          (new DenseVector(Array[Double](
            p_cur(0)*p_cur(3), //n.AB
            p_cur(1)*p_cur(2), //n.Ab
            p_cur(1)*p_cur(2), //n.aB
            p_cur(0)*p_cur(3)  //n.ab
          )) :* gtCounts(4) / ((p_cur(0)*p_cur(3))+(p_cur(1)*p_cur(2))) )
          ) :/ nHaplotypes

      }

      return Some(p_next :* nHaplotypes)

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
      case(counter,v,va,s,sa,i,g) =>
        counter.addGenotype(v.toString(),i,g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (v,va) => triosGeneAnn(va).getOrElse("None").toString}
    ).mapValues({
      case svm => new VariantPairsCounter(svm,ped.value)
    }).persist(StorageLevel.MEMORY_AND_DISK)
    
    //Get unique variants that are found in pairs in our samples
    //TODO: Can this be replaced by a fold?
    val uniqueVariants = triosRDD.map({
      case(gene,svm) => svm.getHetSites()
      }).reduce(
      {case(v1,v2) => v1 ++ v2}
    )

    /**val file = new File(options.output)
    val bw = new BufferedWriter(new FileWriter(file))
    uniqueVariants.foreach({
      case(v) =>  bw.write(v+"\n")
    })
    bw.close()**/

    info("Found " + uniqueVariants.size.toString + " variants in pairs in samples.")

    val bcUniqueVariants = state.sc.broadcast(uniqueVariants)

    def variantsOfInterestFilter = {(v: Variant, va: Annotation) => bcUniqueVariants.value.contains(v.toString)}

    //Load ExAC VDS, filter common samples and sites based on exac condition (AC)
    //val exacVDS = FilterVariants.run(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),Array("-c",options.cexac,"--keep")).vds.
    //  filterSamples((s: String, sa: Annotation) => Filter.keepThis(trioVDS.sampleIds.contains(s), false)).filterVariants(autosomeFilter)

    val exacVDS = State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)).vds.
      filterSamples((s: String, sa: Annotation) => Filter.keepThis(trioVDS.sampleIds.contains(s), false)).filterVariants(variantsOfInterestFilter)

    val exacGeneAnn = exacVDS.queryVA(options.gene_annotation)._2

    val exacRDD = exacVDS.aggregateByAnnotation(partitioner,new SparseVariantSampleMatrix(exacVDS.sampleIds))({
      case(counter,v,va,s,sa,i,g) =>
        counter.addGenotype(v.toString(),i,g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (v,va) => exacGeneAnn(va).getOrElse("None").toString}
    )


    val callsByGene = triosRDD.join(exacRDD)

    //val x = new SparseVector()

    //write results
    new RichRDD(callsByGene.map(
      {case(gene,(trios,exac)) =>
        trios.addExac(exac)
        gene + "\t" + (trios.toString())
      })).writeTable(options.output,header = Some("gene\t" + VariantPairsCounter.getHeaderString()))

    state
  }
}
