package org.broadinstitute.hail.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.variant.{VariantDataset, Genotype, VariantSampleMatrix, Variant}


object Merge {

  def apply(vds1: VariantDataset, vds2: VariantDataset, mergeMode: Int = 1): RDD[((Variant, Int), (Option[Genotype], Option[Genotype]))] = {
    vds1.fullOuterJoin(vds2.expand())
  }

/*  def getSampleConcordance(rdd: ((Variant, Int), (Option[Genotype], Option[Genotype]))): ConcordanceTable = {
     . mapValues{case((v,s),(gt1,gt2)) => (s,(gt1,gt2))}
  }*/
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

object Concordance {
  val possibleTypes:Array[Option[GenotypeType]] = Array(Some(HomRef), Some(Het), Some(HomVar), Some(NoCall), None)
  val typeNames = Map(Some(HomRef) -> "HomRef",Some(Het) -> "Het", Some(HomVar) -> "HomVar",Some(NoCall) -> "NoCall", None -> "None")
  val labels = for (i <- possibleTypes; j <- possibleTypes) yield typeNames.get(i).get + ":" + typeNames.get(j).get

/*  def calledInBoth(gtpair:(Option[Genotype],Option[Genotype])) : Boolean = {
    gtpair match {
      case (Some(_),Some(_)) => true
      case _ => false
    }
  }*/


  def concordanceBySample(rdd:RDD[((Variant,Int),(Option[Genotype],Option[Genotype]))]): RDD[(Int,ConcordanceTable)] = {
    rdd
      .map { case ((v,s),(gt1,gt2)) => (s,(gt1,gt2)) }
      .aggregateByKey[ConcordanceTable](new ConcordanceTable)((comb,gtp) => comb.addCount(gtp._1,gtp._2),(comb1,comb2) => comb1.merge(comb2))
  }

  def concordanceByVariant(rdd:RDD[((Variant,Int),(Option[Genotype],Option[Genotype]))]): RDD[(Variant,ConcordanceTable)] = {
    rdd
      .map { case ((v,s),(gt1,gt2)) => (v,(gt1,gt2)) }
      .aggregateByKey[ConcordanceTable](new ConcordanceTable)((comb,gtp) => comb.addCount(gtp._1,gtp._2),(comb1,comb2) => comb1.merge(comb2))
  }

  def writeSampleConcordance(rdd:RDD[((Variant,Int),(Option[Genotype],Option[Genotype]))],sep:String="\t"): String = {
    val header = s"ID${sep}nVar${sep}Concordance${sep}%s".format(labels.mkString(sep))
    val concordances = concordanceBySample(rdd).map{case(s,ct) => s + sep + ct.writeConcordance(sep)}.collect()
    header + "\n" + concordances.mkString("\n")
  }

  def getVariantString(v:Variant):String = v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt

  def writeVariantConcordance(rdd:RDD[((Variant,Int),(Option[Genotype],Option[Genotype]))],sep:String="\t"): String = {
    val header = s"Variant${sep}nSamples${sep}Concordance${sep}%s".format(labels.mkString(sep))
    val concordances = concordanceByVariant(rdd).map{case(v,ct) => getVariantString(v) + sep + ct.writeConcordance(sep)}.collect()
    header + "\n" + concordances.mkString("\n")
  }

}
