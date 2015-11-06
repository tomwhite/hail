package org.broadinstitute.hail.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.variant.{VariantDataset, Genotype, Variant}

object Merge {
  def apply(vds1: VariantDataset, vds2: VariantDataset): Merge = {
    new Merge(vds1.fullOuterJoin(vds2.expand()))
  }
}

case class Merge(mergeRDD: RDD[((Variant,Int),(Option[Genotype],Option[Genotype]))]) {

  val possibleTypes:Array[Option[GenotypeType]] = Array(Some(HomRef), Some(Het), Some(HomVar), Some(NoCall), None)
  val typeNames = Map(Some(HomRef) -> "HomRef",Some(Het) -> "Het", Some(HomVar) -> "HomVar",Some(NoCall) -> "NoCall", None -> "None")
  val labels = for (i <- possibleTypes; j <- possibleTypes) yield typeNames.get(i).get + ":" + typeNames.get(j).get

  def variantString(v: Variant): String = v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt

  def calledInBoth(gtpair:(Option[Genotype],Option[Genotype])) : Boolean = {
    gtpair match {
      case (Some(_),Some(_)) => true
      case _ => false
    }
  }

  def sampleConcordance: RDD[(Int,ConcordanceTable)] = {
    mergeRDD
      .map { case ((v,s),(gt1,gt2)) => (s,(gt1,gt2)) }
      .aggregateByKey[ConcordanceTable](new ConcordanceTable)((comb,gtp) => comb.addCount(gtp._1,gtp._2),(comb1,comb2) => comb1.merge(comb2))
  }

  def variantConcordance: RDD[(Variant,ConcordanceTable)] = {
    mergeRDD
      .map { case ((v,s),(gt1,gt2)) => (v,(gt1,gt2)) }
      .aggregateByKey[ConcordanceTable](new ConcordanceTable)((comb,gtp) => comb.addCount(gtp._1,gtp._2),(comb1,comb2) => comb1.merge(comb2))
  }

  def applyMergeMode(mergeMode:Int):RDD[((Variant,Int),Option[Genotype])] = {
    def mergeRule1(gt1:Option[Genotype],gt2:Option[Genotype]): Option[Genotype] = {
      val gt1t = gt1 match {
        case Some(gt) => gt.gtType
        case None => NoCall
      }
      val gt2t = gt2 match {
        case Some(gt) => gt.gtType
        case None => NoCall
      }

      if (gt1t == gt2t)
        gt1
      else if (gt1t == NoCall)
        gt2
      else if (gt2t == NoCall)
        gt1
      else
        Some(Genotype(-1,(0,0),0,(0,0,0))) //output if two genotypes conflict
    }

    mergeMode match {
      case 1 => mergeRDD.mapValues { case (gt1, gt2) => mergeRule1(gt1, gt2) } // default -- consensus merging
      case _ => throw new UnsupportedOperationException
    }
  }

  def writeSampleConcordance(sep:String="\t"): String = {
    val header = s"ID${sep}nVar${sep}Concordance${sep}%s".format(labels.mkString(sep))
    val concordances = sampleConcordance.map{case(s,ct) => s + sep + ct.writeConcordance(sep)}.collect()
    header + "\n" + concordances.mkString("\n")
  }

  def writeVariantConcordance(sep:String="\t"): String = {
    val header = s"Variant${sep}nSamples${sep}Concordance${sep}%s".format(labels.mkString(sep))
    val concordances = variantConcordance.map{case(v,ct) => variantString(v) + sep + ct.writeConcordance(sep)}.collect()
    header + "\n" + concordances.mkString("\n")
  }

  def toString(nrow:Int=10): String = { //this doesn't seem to be working...
    def toLine(v:Variant,s:Int,gt1:Option[Genotype],gt2:Option[Genotype]): String = {
      def getGenotypeString(gt:Option[Genotype],v:Variant):String = {
        gt match {
          case Some(gt) => gt.gtString(v)
          case None => "-/-"
        }
      }
      "%s\t%s\t%s\t%s".format(variantString(v), s, getGenotypeString(gt1,v), getGenotypeString(gt2,v))
    }

    mergeRDD
      .take(nrow)
      .map{case ((v,s),(gt1,gt2)) => toLine(v,s,gt1,gt2)}
      .mkString("\n")
  }
}



