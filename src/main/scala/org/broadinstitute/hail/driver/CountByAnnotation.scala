package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.immutable.Map

/**
  * Created by laurent on 4/11/16.
  */
object CountByAnnotation extends Command {

  def name = "countbyannotation"

  def description = "Count number of alternate alleles per sample per given annotation. At the moment, the binning is naive => each value gets a bin!"

  override def supportsMultiallelic = false

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _

    @Args4jOption(required = true, name = "-a", aliases = Array("--annotation"), usage = "annotation to group by")
    var annotation: String = _
  }
  def newOptions = new Options

 /** def getAnnotationType(vds: VariantDataset, annotation: String) : Type = vds.metadata.vaSignature.getOption(annotation) match {
      case None => fatal("annotation" + annotation + "not found in input VDS.")
      case Some(s) =>
  }**/

  def run(state: State, options: Options): State = {
    val vds = state.vds

    //vds.metadata.vaSignature.

    //val annnotationType = getAnnotationType(vds,options.annotation)


    if(vds.metadata.vaSignature.getOption(options.annotation).isEmpty){

    }

   // vds.aggregateByVariantWithKeys(Map[vds.metadata.vaSignature.getOption(options.annotation).getClass(), String]())

    // query sa.case, must be boolean
    val qCase = vds.querySA("case")

    // insert nCase, then nControl
    val (tmpVAS, insertCase) = vds.vaSignature.insert(TInt, "nCase")
    val (newVAS, insertControl) = tmpVAS.insert(TInt, "nControl")

    val saBc = state.sc.broadcast(vds.sampleAnnotations)

    state.copy(vds =
      vds.mapAnnotationsWithAggregate((0, 0), newVAS)({ case ((nCase, nControl), v, s, g) =>
        val isCase = qCase(saBc.value(s))
        (g.nNonRefAlleles, isCase) match {
          case (Some(n), Some(true)) => (nCase + n, nControl)
          case (Some(n), Some(false)) => (nCase, nControl + n)
          case _ => (nCase, nControl)
        }
      }, { case ((nCase1, nControl1), (nCase2, nControl2)) =>
        (nCase1 + nCase2, nControl1 + nControl2)
      }, { case (va, (nCase, nControl)) =>
        // same order as signature insertion above
        insertControl(insertCase(va, Some(nCase)),
          Some(nControl))
      }))
  }
}
