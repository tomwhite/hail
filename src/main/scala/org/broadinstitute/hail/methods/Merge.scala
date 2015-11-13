package org.broadinstitute.hail.methods

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.variant.{VariantDataset, Genotype, Variant}
import scala.collection.mutable.Map

object Merge {
  def apply(vds1: VariantDataset, vds2: VariantDataset, sc: SparkContext): Merge = {
    val ids1 = vds1.sampleIds.zipWithIndex.toMap
    val idConv: scala.collection.mutable.Map[Int, Int] = scala.collection.mutable.Map[Int, Int]()
    var index = ids1.size
    val masterSampleIds: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int,String]()

    for ((s,i) <- ids1) {
      masterSampleIds(i) = s
    }

    for ((s, i) <- vds2.sampleIds.zipWithIndex) {
      ids1.lift(s) match {
        case Some(mapping) =>
          idConv += i -> mapping
        case None =>
          idConv += i -> index
          masterSampleIds += index -> s
          index += 1
      }
    }


    new Merge(vds1.fullOuterJoin(vds2
      .expand()
      .map{case (v,s,g) => (v,idConv(s), g)}),
      sc,
      masterSampleIds)
  }

  def calledInBoth(gtpair:(Option[Genotype],Option[Genotype])) : Boolean = {
    gtpair match {
      case (Some(_),Some(_)) => true
      case _ => false
    }
  }

  def getGenotypeType(gt:Option[Genotype]):GenotypeType = {
    gt match {
      case Some(gt) => gt.gtType
      case None => NoCall
    }
  }

  def mergeRule1(gt1:Option[Genotype],gt2:Option[Genotype]): Option[Genotype] = {
    val gt1t = getGenotypeType(gt1)
    val gt2t = getGenotypeType(gt2)

    if (gt1t == gt2t)
      gt1
    else if (gt1t == NoCall)
      gt2
    else if (gt2t == NoCall)
      gt1
    else
      Some(Genotype(-1,(0,0),0,(0,0,0))) //output if two genotypes conflict
  }

  def mergeRule2(gt1:Option[Genotype],gt2:Option[Genotype]):Option[Genotype] = {
    val gt1t = getGenotypeType(gt1)
    val gt2t = getGenotypeType(gt2)

    if (gt1t != NoCall)
      gt1
    else
      gt2
  }

  def mergeRule3(gt1:Option[Genotype],gt2:Option[Genotype]):Option[Genotype] = {
    val gt1t = getGenotypeType(gt1)
    val gt2t = getGenotypeType(gt2)

    if (gt2t != NoCall)
      gt2
    else
      gt1
  }

  def mergeRule4(gt1:Option[Genotype],gt2:Option[Genotype]):Option[Genotype] = {
    gt1
  }

  def mergeRule5(gt1:Option[Genotype],gt2:Option[Genotype]):Option[Genotype] = {
    gt2
  }

}

class Merge(mergeRDD: RDD[((Variant,Int),(Option[Genotype],Option[Genotype]))],sc:SparkContext,sampleIds:Map[Int,String]) {
  import Merge._

  val possibleTypes:Array[Option[GenotypeType]] = Array(Some(HomRef), Some(Het), Some(HomVar), Some(NoCall), None)
  val typeNames = Map(Some(HomRef) -> "HomRef",Some(Het) -> "Het", Some(HomVar) -> "HomVar",Some(NoCall) -> "NoCall", None -> "None")
  val labels = for (i <- possibleTypes; j <- possibleTypes) yield typeNames.get(i).get + ":" + typeNames.get(j).get

  def variantString(v: Variant): String = v.contig + ":" + v.start + ":" + v.ref + ":" + v.alt



  def applyMergeMode(mergeMode:Int):RDD[((Variant,Int),Option[Genotype])] = {
    mergeMode match {
      case 1 => mergeRDD.mapValues { case (gt1, gt2) => mergeRule1(gt1, gt2) } // default -- consensus merging
      case 2 => mergeRDD.mapValues { case (gt1,gt2) => mergeRule2(gt1,gt2)} // only use gt2 if gt1 == no call
      case 3 => mergeRDD.mapValues { case (gt1,gt2) => mergeRule3(gt1,gt2)} // only use gt1 if gt2 == no call
      case 4 => mergeRDD.mapValues { case (gt1,gt2) => mergeRule4(gt1,gt2)} // do not overwrite gt1
      case 5 => mergeRDD.mapValues { case (gt1,gt2) => mergeRule5(gt1,gt2)} // do not overwrite gt2
      case _ => throw new UnsupportedOperationException
    }
  }

  def sampleConcordance: RDD[(Int,ConcordanceTable)] = {
    mergeRDD
      .filter{case ((v,s),(gt1,gt2)) => calledInBoth((gt1,gt2))}
      .map { case ((v,s),(gt1,gt2)) => (s,(gt1,gt2)) }
      .aggregateByKey[ConcordanceTable](new ConcordanceTable)((comb,gtp) => comb.addCount(gtp._1,gtp._2),(comb1,comb2) => comb1.merge(comb2))
  }

  def variantConcordance: RDD[(Variant,ConcordanceTable)] = {
    mergeRDD
      .filter{case ((v,s),(gt1,gt2)) => calledInBoth((gt1,gt2))}
      .map { case ((v,s),(gt1,gt2)) => (v,(gt1,gt2)) }
      .aggregateByKey[ConcordanceTable](new ConcordanceTable)((comb,gtp) => comb.addCount(gtp._1,gtp._2),(comb1,comb2) => comb1.merge(comb2))
  }

  def writeSampleConcordance(filename:String,sep:String="\t"): String = {
    val header = s"ID${sep}nVar${sep}Concordance${sep}%s".format(labels.mkString(sep))
    val lines = sampleConcordance.map{case(s,ct) => s + sep + ct.writeConcordance(sep)}.collect()
    header + "\n" + lines.mkString("\n")
    //writeTable(filename, sc.hadoopConfiguration, lines, header)
  }

  def writeVariantConcordance(filename:String,sep:String="\t"):String = {
    val header = s"Variant${sep}nSamples${sep}Concordance${sep}%s".format(labels.mkString(sep))
    val lines = variantConcordance.map{case(v,ct) => variantString(v) + sep + ct.writeConcordance(sep)}.collect()
    header + "\n" + lines.mkString("\n")
    //writeTable(filename, sc.hadoopConfiguration, lines, header)
  }

  def pretty(nrow:Int=10): String = {
    def toLine(v:Variant,s:Int,gt1:Option[Genotype],gt2:Option[Genotype]): String = {
      def getGenotypeString(gt:Option[Genotype],v:Variant):String = {
        gt match {
          case Some(gt) => gt.gtString(v)
          case None => "-/-"
        }
      }
      "%s\t%s\t%s\t%s".format(variantString(v), sampleIds.get(s).get, getGenotypeString(gt1,v), getGenotypeString(gt2,v))
    }

    mergeRDD
      .take(nrow)
      .map{case ((v,s),(gt1,gt2)) => toLine(v,s,gt1,gt2)}
      .mkString("\n")
  }
}

