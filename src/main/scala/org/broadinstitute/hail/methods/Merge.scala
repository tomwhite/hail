package org.broadinstitute.hail.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.variant.{VariantDataset, Genotype, VariantSampleMatrix, Variant}
/*
class Merge (vds1:VariantSampleMatrix[Genotype], vds2: VariantSampleMatrix[Genotype], mergeMode: Int=1) {
  val vds3 = vds1.fullOuterJoin(vds2.expand())

  override def toString: String = {
    /*def getData(v:Variant,s:Int,gt1:Option[Genotype],gt2:Option[Genotype]): String = {
      val a1 = Array(v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt,s,gt1,gt2)
      a1.mkString("\t")
    }*/

    def variantString(v: Variant): String = v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt

    def getData(v:Variant):String = variantString(v) + "\n"

    val header = "Variant\tSample\tGT1\tGT2\n"
    //val varnames = vds3.map { case (k,v)  => getData(k._1)}.collect()
//    val varnames = vds3.map{case (k,v) => getData()}.collect()


    header + varnames.mkString("\n")
    //val data = vds3.foreach{case(k,v) => getData(k._1,k._2,v._1,v._2)}
    //for ((k,v) <- vds_merge) yield getData(k,v)

    //val data = for ((k,v) <- vds3) yield {getData(k._1,k._2)}

    //header + data.mkString("\n")
  }
}
*/
object Merge {

  def apply(vds1: VariantDataset, vds2: VariantDataset, mergeMode: Int = 1): RDD[((Variant, Int), (Option[Genotype], Option[Genotype]))] = {
    vds1.fullOuterJoin(vds2.expand())
  }

  def getConcordance(): ConcordanceTable = throw new UnsupportedOperationException


  def mergeRule1(gts:(GenotypeType,GenotypeType)): GenotypeType = {
    if (gts._1 == gts._2)
      gts._1
    else if (gts._1 == NoCall)
      gts._2
    else if (gts._2 == NoCall)
      gts._1
    else
      NoCall
  }

  def mergeRule2(gts:(GenotypeType,GenotypeType)): GenotypeType = if (gts._1 != NoCall) gts._1 else gts._2
  def mergeRule3(gts:(GenotypeType,GenotypeType)): GenotypeType = if (gts._2 != NoCall) gts._2 else gts._1
  def mergeRule4(gts:(GenotypeType,GenotypeType)): GenotypeType = gts._1
  def mergeRule5(gts:(GenotypeType,GenotypeType)): GenotypeType = gts._2

/*  def getMergeGenotype(mergeMode:Int=1,gts:(Option[GenotypeType],Option[GenotypeType])):Option[GenotypeType] = {
    mergeMode match {
      case 1 => mergeRule1(gts) // default -- consensus merging
      case 2 => mergeRule2(gts) // vds1 + NoCall --> vds2
      case 3 => mergeRule3(gts) // vds2 + NoCall --> vds1; Not sure I interpreted this correctly
      case 4 => mergeRule4(gts) // vds1
      case 5 => mergeRule5(gts) // vds2
    }
  }*/
}