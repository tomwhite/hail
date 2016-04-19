package org.broadinstitute.hail.utils

import org.broadinstitute.hail.variant.{Genotype, GenotypeType}
import org.broadinstitute.hail.variant.GenotypeType._

import scala.collection.mutable

/**
  * Created by laurent on 4/19/16.
  */
class SparseVariantSampleMatrix(sampleIDs: IndexedSeq[String]) extends Serializable {

  val nSamples = sampleIDs.length
  //var variantSampleIndex = 0

  private val variants = mutable.HashMap[String, mutable.HashMap[String,GenotypeType]]()
  private val samples = mutable.HashMap[String, mutable.HashMap[String, GenotypeType]]()

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
        variants.get(variantID).get.update(sampleID, g.gtType)
      }else{
        variants.update(variantID,mutable.HashMap(sampleID -> g.gtType))
      }

      //Add variant in sample
      if(samples.contains(sampleID)){
        samples.get(sampleID).get.update(variantID,g.gtType)
      }else{
        samples.update(sampleID,mutable.HashMap(variantID -> g.gtType))
      }

    }

    //variants = variants.updated(v, mv)
    this
  }

  def getVariant(variantID: String): Option[Map[String,GenotypeType]] = {
    if(variants.contains(variantID)) {
      Some(variants.get(variantID).get.toMap)
    }
    None
  }

  def getSample(sampleID: String): Option[Map[String,GenotypeType]] = {
    if(sampleIDs.contains(sampleID)) {
      if(samples.contains(sampleID)) {
        Some(samples.get(sampleID).get.toMap)
      }else{
        Some(Map[String,GenotypeType]())
      }
    }
    None
  }

  def getGenotype(variantID: String, sampleID:String) : Option[GenotypeType] = {
    if (variants.contains(variantID) && sampleIDs.contains(sampleID)) {
      variants.get(variantID).get.get(sampleID) match {
        case (Some(g)) => Some(g)
        case None => Some(GenotypeType.HomRef)
      }
    }
    None
  }

}
