package org.broadinstitute.hail.driver

import org.kohsuke.args4j.{Option => Args4jOption}
import scala.language.postfixOps

object MergeCommand extends Command {
  def name = "merge"

  def description = "Merge two VDS and calculate concordance statistics"

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "-o", aliases = Array("--output"), usage = "Output root filename")
    var output: String = _

    @Args4jOption(required = false, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _
  }

  def newOptions = new Options

  def run(state: State, options: Options): State = {
    println("hello merge")
    state
  }
}

 /* class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output root filename")
    var output: String = _

    @Args4jOption(required = true, name = "-f", aliases = Array("--fam"), usage = ".fam file")
    var famFilename: String = _
  }
  def newOptions = new Options

  def run(state: State, options: Options): State = {
/*    val vds = state.vds
    val ped = Pedigree.read(options.famFilename, vds.sampleIds)
    val men = MendelErrors(vds, ped)

    val result1 = "rm -rf " + options.output + ".mendel" !;
    val result2 = "rm -rf " + options.output + ".lmendel" !;

    men.writeMendel(options.output + ".mendel")
    men.writeMendelL(options.output + ".lmendel")
    men.writeMendelF(options.output + ".fmendel")
    men.writeMendelI(options.output + ".imendel")*/
*/
/*    state
  }*/
//}


