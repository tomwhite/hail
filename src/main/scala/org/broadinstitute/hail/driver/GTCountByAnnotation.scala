package org.broadinstitute.hail.driver

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.expr._
import org.broadinstitute.hail.utils.MultiArray2
import org.broadinstitute.hail.variant._
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import scala.collection.JavaConverters._

import scala.collection.immutable.Map

/**
  * Created by laurent on 4/11/16.
  */
object GTCountByAnnotation extends Command {

  def name = "countbyannotation"

  def description = "Count number of alternate alleles per sample per given annotation. At the moment, the binning is naive => each value gets a bin!"

  override def supportsMultiallelic = false

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-o", aliases = Array("--output"), usage = "Output filename")
    var output: String = _

    @Args4jOption(required = true, name = "-a", aliases = Array("--annotation"), usage = "annotations ... ")
    var annotations: Array[String] = _

    //@Argument(required = true, name = "-a", aliases = Array("--annotation"), usage = "<annotations...>")
    //var annotations: java.util.ArrayList[String] = _
  }
  def newOptions = new Options

 /** def getAnnotationType(vds: VariantDataset, annotation: String) : Type = vds.metadata.vaSignature.getOption(annotation) match {
   * case None => fatal("annotation" + annotation + "not found in input VDS.")
   * case Some(s) =>
   * }**/




 class GTCounter extends Serializable{

   var nHet = 0;
   var nVar = 0;
   var nRef = 0;
   var nMissing = 0;

  def merge(gc: GTCounter) : GTCounter = {
    nHet += gc.nHet
    nVar += gc.nVar
    nRef += gc.nRef
    nMissing += gc.nMissing
    this
  }
   //def seqOpU, Variant, Annotation, String, Annotation, T) => U,

   def addGenotype(g: Genotype) : GTCounter ={

     g.gt match {

       case Some(0) => nRef += 1
       case Some (1) => nHet += 1
       case Some(2) => nVar +=1
       case None => nMissing += 1


     }
     this
   }

 }


  /**class AggAnn{

    override def equals(other: Any): Boolean = other match{
      case that: AggAnn =>
    }

    val annotations = Map[String,Annotation]


  }**/

  def run(state: State, options: Options): State = {
    val vds = state.vds

    //vds.metadata.vaSignature.

    //val annnotationType = getAnnotationType(vds,options.annotation)

//    val annotations = Parser.parseAnnotationRootList(options.annotation, Annotation.VARIANT_HEAD)

    val annList = options.annotations.map(a => vds.queryVA(a)).toSeq

   /** if(vds.metadata.vaSignature.getOption(options.annotation).isEmpty){

    }**/

    //val ann = state.sc.broadcast(vds.queryVA(options.annotation))
    //val vaBc = state.sc.broadcast(vds.metadata.)

    val annAgg = vds.aggregateByAnnotation(new GTCounter())({
      case(counter,v,va,s,sa,g) =>
        counter.addGenotype(g)},{
      case(c1,c2) => c1.merge(c2)},
      {case (va) =>
        annList.map(a => a(va))
        }
    ).collect()




   state
  }

}
