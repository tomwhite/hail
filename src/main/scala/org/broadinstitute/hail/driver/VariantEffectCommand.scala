package org.broadinstitute.hail.driver

import org.broadinstitute.hail.methods.{Vep, CovariateData, LinearRegression, Pedigree}
import org.kohsuke.args4j.{Option => Args4jOption}

object VariantEffectCommand extends Command {

  def name = "vep"
  def description = "Run VEP"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-vc", aliases = Array("--vep-config"), usage = "Vep configuration directory")
    var vepConfig: String = _

  }
  def newOptions = new Options

  def run(state: State, options: Options): State = {
    new Vep(options.vepConfig)
        .apply(state.vds)
        .take(1)
        .foreach(println)

    state
  }
}
