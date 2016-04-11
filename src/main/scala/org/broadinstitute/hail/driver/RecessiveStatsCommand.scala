package org.broadinstitute.hail.driver

import org.broadinstitute.hail.variant.{VariantDataset, VariantSampleMatrix}
import org.kohsuke.args4j.{Option, Option => Args4jOption}

/**
  * Created by laurent on 4/8/16.
  */
object RecessiveStatsCommand extends Command {

    def name = "recessive stats"

    def description = "Computes the a per-gene Poisson test on enrichment for rare functional variation give a set of cases and controls"
    override def supportsMultiallelic = true

    class Options extends BaseOptions {
      @Args4jOption(required = true, name = "-i", aliases = Array("--controls"), usage = "Input control .vds file")
      var controls_input: String = _
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


    def run(state: State, options: Options): State = {


      val controls_input = options.controls_input

      val casesVDS = filterVDS(state,options)
      val controlVDS = filterVDS(State(state.sc,state.sqlContext,VariantSampleMatrix.read(state.sqlContext, options.controls_input)),options)


      state
    }
}
