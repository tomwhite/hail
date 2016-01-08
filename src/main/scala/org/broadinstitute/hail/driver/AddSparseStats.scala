package org.broadinstitute.hail.driver

import org.broadinstitute.hail.methods.{CovariateData, Pedigree, SparseStats}
import org.kohsuke.args4j.{Option => Args4jOption}

object AddSparseStats extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _

    @Args4jOption(required = true, name = "-c", aliases = Array("--cov"), usage = ".cov file")
    var covFilename: String = _
  }
  def newOptions = new Options

  def name = "sparsestats"
  def description = "Add an RDD of (Variant, SparseStat) to the state."
  def run(state: State, options: Options): State = {
    val vds = state.vds
    val ped = Pedigree.read(options.famFilename, state.hadoopConf, vds.sampleIds)
    val cov = CovariateData.read(options.covFilename, state.hadoopConf, vds.sampleIds)

    state.copy(sparseStats = SparseStats(vds, ped, cov).cache())
  }
}
