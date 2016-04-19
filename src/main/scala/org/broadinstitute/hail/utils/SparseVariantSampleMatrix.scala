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

  def merge(g: SparseVariantSampleMatrix): SparseVariantSampleMatrix = {

    variants ++= g.variants

    g.samples foreach {case(s,variants) => {
      if(samples.contains(s)){
        samples.get(s).get ++= variants
      }else{
        samples.update(s,variants)
      }
    }}
    this
  }

  def addVariantGenotype(v: String, s: String, g: Genotype): SparseVariantSampleMatrix = {

    if(!g.isHomRef) {
      //Add genotype in variants
      if (variants.contains(v)) {
        variants.get(v).get.update(s, g.gtType)
      }else{
        variants.update(v,mutable.HashMap(s -> g.gtType))
      }

      //Add variant in sample
      if(samples.contains(s)){
        samples.get(s).get.update(v,g.gtType)
      }else{
        samples.update(s,mutable.HashMap(v -> g.gtType))
      }

    }

    //variants = variants.updated(v, mv)
    this
  }

  def getVariant(v: String): Option[Map[String,GenotypeType]] = {
    if(variants.contains(v)) {
      Some(variants.get(v).get.toMap)
    }
    None
  }

  def getSample(s: String): Option[Map[String,GenotypeType]] = {
    if(sampleIDs.contains(s)) {
      if(samples.contains(s)) {
        Some(samples.get(s).get.toMap)
      }else{
        Some(Map[String,GenotypeType]())
      }
    }
    None
  }

  def getGenotype(v: String, s:String) : Option[GenotypeType] = {
    if (variants.contains(v) && sampleIDs.contains(s)) {
      variants.get(v).get.get(s) match {
        case (Some(g)) => Some(g)
        case None => Some(GenotypeType.HomRef)
      }
    }
    None
  }

}
