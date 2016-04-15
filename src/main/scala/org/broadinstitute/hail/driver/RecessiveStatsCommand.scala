package org.broadinstitute.hail.driver

import org.broadinstitute.hail.variant.{Genotype, VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.variant.GenotypeType._

import scala.collection.mutable

/**
  * Created by laurent on 4/8/16.
  */
object RecessiveStatsCommand extends Command {

    def name = "recessive stats"

    def description = "Computes the a per-gene Poisson test on enrichment for rare functional variation give a set of cases and controls"
    override def supportsMultiallelic = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "-i", aliases = Array("--controls"), usage = "Input controls .vds file")
      var controls_input: String = _
      @Args4jOption(required = true, name = "-a", aliases = Array("--gene_annotation"), usage = "Annotation storing the gene information for aggregation")
      var gene_annotation: String = _
      @Args4jOption(required = false, name = "-cg", aliases = Array("--condition_genotypes"), usage = "Expression for filtering genotypes")
      var cg : String = ""
      @Args4jOption(required = false, name = "-cv", aliases = Array("--condition_variants"), usage = "Expression for filtering variants (non-filtered variants are considered functional)")
      var cv : String = ""

    }

    def newOptions = new Options

    def filterVDS(state: State, options: Options) : VariantDataset = {

      var filteredState = state
      if(options.cg.nonEmpty) {
        filteredState = FilterGenotypes.run(state, Array("-c", options.cg))
      }

      if(options.cv.nonEmpty){
        filteredState = FilterVariants.run(state,Array("-c",options.cv))
      }

      return filteredState.vds
    }


  class SparseVariantSampleMatrix(sampleIDs: IndexedSeq[String]) extends Serializable {

    val nSamples = sampleIDs.length
    //var variantSampleIndex = 0

    private val variants = mutable.HashMap[String, mutable.HashMap[String,GenotypeType]]()
    private val samples = mutable.HashMap[String, mutable.HashMap[String, GenotypeType]]()

    def merge(g: SparseVariantSampleMatrix): SparseVariantSampleMatrix = {

      variants ++= g.variants

      g.samples foreach {case(s,variants) => {
        if(samples.contains(s)){
          samples.get(s).get ++= variants
        }else{
          samples.update(s,variants)
        }
      }}
      this
    }

    def addVariantGenotype(v: String, s: String, g: Genotype): SparseVariantSampleMatrix = {

      if(!g.isHomRef) {
        //Add genotype in variants
        if (variants.contains(v)) {
          variants.get(v).get.update(s, g.gtType)
        }else{
          variants.update(v,mutable.HashMap(s -> g.gtType))
        }

        //Add variant in sample
        if(samples.contains(s)){
          samples.get(s).get.update(v,g.gtType)
        }else{
          samples.update(s,mutable.HashMap(v -> g.gtType))
        }

      }

      //variants = variants.updated(v, mv)
      this
    }

    def getVariant(v: String): Option[Map[String,GenotypeType]] = {
      if(variants.contains(v)) {
        Some(variants.get(v).get.toMap)
      }
      None
    }

    def getSample(s: String): Option[Map[String,GenotypeType]] = {
      if(sampleIDs.contains(s)) {
        if(samples.contains(s)) {
          Some(samples.get(s).get.toMap)
        }else{
          Some(Map[String,GenotypeType]())
        }
      }
      None
    }

    def getGenotype(v: String, s:String) : Option[GenotypeType] = {
      if (variants.contains(v) && sampleIDs.contains(s)) {
        variants.get(v).get.get(s) match {
          case (Some(g)) => Some(g)
          case None => Some(GenotypeType.HomRef)
        }
      }
      None
    }

  }


    def run(state: State, options: Options): State = {


      val controls_input = options.controls_input

      val casesVDS = filterVDS(state,options)
      val controlsVDS = filterVDS(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),options)

      val casesGeneAnn = casesVDS.queryVA(options.gene_annotation)._2
      val controlsGeneAnn = controlsVDS.queryVA(options.gene_annotation)._2

      val casesGT = casesVDS.aggregateByAnnotation(new SparseVariantSampleMatrix(casesVDS.sampleIds))({
        case(counter,v,va,s,sa,g) =>
          counter.addVariantGenotype(s,v.toString(),g)},{
        case(c1,c2) => c1.merge(c2)},
        {case (va) => casesGeneAnn(va)}
      )
/**
  * val controlsGT = controlsVDS.aggregateByAnnotation(new MinimalVariantSampleCounter())({
  * case(counter,v,va,s,sa,g) =>
  * counter.addVariantGenotype(s,v.toString(),g)},{
  * case(c1,c2) => c1.merge(c2)},
  * {case (va) => controlsGeneAnn(va)}
  * )
**/



      state
    }
}
