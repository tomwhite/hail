package org.broadinstitute.hail.driver

import org.broadinstitute.hail.methods.{LinearRegressionFromSparseStats, CovariateData, LinearRegression, Pedigree}
import org.kohsuke.args4j.{Option => Args4jOption}

object LinearRegressionFromSparseStatsCommand extends Command {

  def name = "linregss"
  def description = "Compute beta, std error, t-stat, and p-val for each SNP with additional sample covariates"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output root filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _

    @Args4jOption(required = true, name = "-c", aliases = Array("--cov"), usage = ".cov file")
    var covFilename: String = _
  }
  def newOptions = new Options

  def run(state: State, options: Options): State = {
    val ss = state.sparseStats

    require(ss != null)

    val vds = state.vds
    val ped = Pedigree.read(options.famFilename, state.hadoopConf, vds.sampleIds)
    val cov = CovariateData.read(options.covFilename, state.hadoopConf, vds.sampleIds)
    val linreg = LinearRegressionFromSparseStats(ss, ped, cov).lr.collect()

    //linreg.write(options.output)

    state
  }
}
