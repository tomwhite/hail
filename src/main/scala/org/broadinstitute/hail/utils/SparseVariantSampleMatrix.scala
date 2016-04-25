package org.broadinstitute.hail.utils

import org.broadinstitute.hail.variant.{Genotype, GenotypeType}
import org.broadinstitute.hail.variant.GenotypeType._

import scala.collection.mutable

/**
  * Created by laurent on 4/19/16.
  */
class SparseVariantSampleMatrix(val sampleIDs: IndexedSeq[String]) extends Serializable {

  val nSamples = sampleIDs.length
  private var variantIDs = IndexedSeq[String]()
  //var variantSampleIndex = 0

  val variants = mutable.Map[String, mutable.Map[String,Genotype]]()
  val samples = mutable.Map[String, mutable.Map[String, Genotype]]()

  def merge(that: SparseVariantSampleMatrix): SparseVariantSampleMatrix = {

    variants ++= that.variants

    that.samples foreach {case(s,variants) => {
      if(samples.contains(s)){
        samples.get(s).get ++= variants
      }else{
        samples.update(s,variants)
      }
    }}
    this
  }

  def addVariantGenotype(variantID: String, sampleID: String, g: Genotype): SparseVariantSampleMatrix = {

    if(!g.isHomRef) {
      //Add genotype in variants
      if (variants.contains(variantID)) {
        variants.get(variantID).get.update(sampleID, Genotype(g.gt))
      }else{
        variants.update(variantID,mutable.HashMap(sampleID -> Genotype(g.gt)))
      }

      //Add variant in sample
      if(samples.contains(sampleID)){
        samples.get(sampleID).get.update(variantID,Genotype(g.gt))
      }else{
        samples.update(sampleID,mutable.HashMap(variantID -> Genotype(g.gt)))
      }

    }

    //variants = variants.updated(v, mv)
    this
  }

  def getVariant(variantID: String): Option[Map[String,Genotype]] = {
    variants.get(variantID) match{
      case Some(variant) => Some(variant.toMap)
      case None => None
    }
  }

  def getSample(sampleID: String): Option[Map[String,Genotype]] = {

    if(!sampleIDs.contains(sampleID)) { return None }

    if(samples.contains(sampleID)) {
      Some(samples.get(sampleID).get.toMap)
    }else{
      Some(Map[String,Genotype]())
    }

  }

  def getGenotype(variantID: String, sampleID:String) : Option[Genotype] = {

    if (!variants.contains(variantID) || !sampleIDs.contains(sampleID)) { return None }

    variants.get(variantID).get.get(sampleID) match {
      case (Some(g)) => Some(g)
      case None => Some(Genotype(0)) //TODO find a way of not hardcoding this
    }
  }

  def getAC(variantID: String) : Int ={

    variants.get(variantID) match{
      case Some(sampleGenotypes) => sampleGenotypes.foldLeft(0)({
        case(acc,(s,gt)) =>
          if(gt.isHet){acc + 1}
          else if(gt.isHomVar){acc + 2}
          else{acc}
      })
      case None => 0
    }

  }

  def cumulativeAF: Double = {

    variants.aggregate(0.0)({(acc, variant) =>
      //Count the number of called samples and the number of non-ref alleles
      val counts = variant._2.foldLeft((0.0,0.0))({(acc2,g) =>
        g match {
          case GenotypeType.NoCall => (acc2._1, acc2._2 + 1)
          case GenotypeType.Het => (acc2._1 + 1, acc2._2)
          case GenotypeType.HomVar => (acc2._1 + 2, acc2._2)
          case GenotypeType.HomRef => acc2 //This is only here for completeness sake and should never be used
        }
      })
      counts._1/(nSamples - counts._2)
    },
      {(acc1,acc2) => (acc1 + acc2)
    })

  }

}
