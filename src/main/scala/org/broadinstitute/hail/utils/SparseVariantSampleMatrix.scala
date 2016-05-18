package org.broadinstitute.hail.utils

import java.nio.ByteBuffer

import breeze.linalg.SparseVector
import org.apache.spark.{Partitioner, SparkEnv}
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.annotations.Annotation
import org.broadinstitute.hail.variant.{Genotype, GenotypeType, Variant, VariantSampleMatrix}
import org.broadinstitute.hail.variant.GenotypeType._

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder, ListBuffer, Map}
import scala.reflect.ClassTag
import org.broadinstitute.hail.Utils._


/**
  * Created by laurent on 4/19/16.
  */

object SparseVariantSampleMatrixRRDBuilder {

  //Given a mapping from a variant and its annotations to use as the key to the resulting PairRDD,
  //Aggregates the data in a SparseSampleVariantMatrix
  def buildByAnnotation[K](vsm: VariantSampleMatrix[Genotype], partitioner : Partitioner)(
    mapOp: (Variant, Annotation)  => K)(implicit uct: ClassTag[K]): RDD[(K, SparseVariantSampleMatrix)] = {

    vsm.rdd
      .mapPartitions { (it: Iterator[(Variant, Annotation, Iterable[Genotype])]) =>
        val gtBuilder = new mutable.ArrayBuilder.ofByte()
        val siBuilder = new ArrayBuilder.ofInt()
        it.map { case (v, va, gs) =>
          gtBuilder.clear()
          siBuilder.clear()
          val sg = gs.iterator.zipWithIndex.foldLeft((siBuilder,gtBuilder))({
            case (acc,(g,i)) => if(!g.isHomRef) (acc._1 += i,  acc._2 += g.gt.getOrElse(-1).toByte) else acc
          })
          (mapOp(v,va), (v.toString,siBuilder.result(),gtBuilder.result()))
        }
      }.aggregateByKey(new SparseVariantSampleMatrix(vsm.sampleIds), partitioner) (
      { case (svsm, (v,sampleIndices,genotypes)) => svsm.addVariant(v,sampleIndices,genotypes) },
      { (svsm1,svsm2) => svsm1.merge(svsm2) })
  }

}

class SparseVariantSampleMatrix(val sampleIDs: IndexedSeq[String]) extends Serializable {

  val nSamples = sampleIDs.length
  val samplesIndex = sampleIDs.zipWithIndex.toMap

  val variants = ArrayBuffer[String]()
  val variantsIndex = mutable.Map[String,Int]()

  //Stores the variants -> sample mappings
  //Populated when adding variants
  private val v_sindices = ArrayBuffer[Int]()
  private val v_genotypes = ArrayBuffer[Byte]()
  private val vindices = ArrayBuffer[Int]()

  //Stores the samples -> variants mappings
  //Lazily computed from variants -> sample mappings
  //when accessing per-sample data
  private val s_vindices = ArrayBuffer[Int]()
  private val s_genotypes = ArrayBuffer[Byte]()
  private val sindices = ArrayBuffer[Int]()

  def nGenotypes() : Int = {
    v_genotypes.size
  }

  def addVariant(variant: String, genotypes: Iterable[Genotype]) : SparseVariantSampleMatrix = {

    var sindex = 0
    genotypes.foreach( gt => {
        addGenotype(variant,sindex,gt)
        sindex += 1
      }
    )
    this
  }

  def addVariant(variant: String, samples: Array[Int], genotypes: Array[Byte]) : SparseVariantSampleMatrix = {

    vindices += v_genotypes.size
    variantsIndex.update(variant,variants.size)
    variants += variant
    v_sindices ++= samples
    v_genotypes ++= genotypes

    this
  }


  def addGenotype(variant: String, index: Int, genotype: Genotype) : SparseVariantSampleMatrix ={

    //Only record non-0 genotypes
    if(!genotype.isHomRef) {
        //Check if adding genotypes to last variant or not
        if(variants.size < 1 || variants.last != variant){
          //If the variant is already present, then update indices appropriately
          val vindex = variants.indexOf(variant)
          if (vindex > 0) {
            //Find the exact place to insert the genotype (keep order)
            var gindex = vindices(vindex)
            while(gindex < vindices(vindex+1) && v_sindices(gindex) < index){gindex += 1}

            //Add the genotype
            v_genotypes.insert(gindex,genotype.gt.getOrElse(-1).toByte)
            v_sindices.insert(gindex,index)
            Range(vindex+1,vindices.size).foreach({
              case i => vindices.update(i, vindices(i)+1)
            })

          }
          //Otherwise append
          else {
            vindices += v_genotypes.size
            variantsIndex.update(variant,variants.size)
            variants += variant
            v_sindices += index
            v_genotypes += genotype.gt.getOrElse(-1).toByte
          }
        }
        //Append genotypes to last variant
        else{
          v_sindices += index
          v_genotypes += genotype.gt.getOrElse(-1).toByte
        }
    }
    this
  }

  def merge(that: SparseVariantSampleMatrix): SparseVariantSampleMatrix = {
    this.vindices ++= that.vindices.map({x => x + this.v_sindices.size})
    that.variantsIndex.foreach({
      case (k,v) => variantsIndex.update(k,v+variants.size)
    })
    this.variants ++= that.variants
    this.v_sindices ++= that.v_sindices
    this.v_genotypes ++= that.v_genotypes
    this
  }

  //Returns None in case the variant is not present
  def getVariantAsOption(variantID: String) : Option[Map[String,Genotype]] = {
    variantsIndex.get(variantID) match{
      case Some(vindex) => Some(getVariant(vindex))
      case None => None
    }
  }

  //Return an empty map in case the variant is not present
 def getVariant(variantID: String): Map[String,Genotype] = {
   getVariant(variantsIndex.getOrElse(variantID, -1))
 }

  def getVariant(variantIndex: Int): Map[String,Genotype] = {

    val variant = mutable.Map[String,Genotype]()

    if(variantIndex > -1) {

      val nextVariantIndex = if (vindices.size > variantIndex + 1) vindices(variantIndex + 1) else v_sindices.size

      Range(vindices(variantIndex), nextVariantIndex).foreach({
        case i => variant.update(sampleIDs(v_sindices(i)), Genotype(v_genotypes(i)))
      })
    }

    return variant

  }

  //Returns None if the sample is absent,
  // a Map of Variants -> Genotypes for that sample otherwise
  def getSampleAsOption(sampleID: String) : Option[Map[String,Genotype]] = {

    val sampleIndex = samplesIndex.getOrElse(sampleID,-1)

    if(sampleIndex < 0) { return None }

    Some(getSample(sampleIndex))

  }

  //Returns a Map of Variants -> Genotype for that sample
  //In case of an absent sample, returns an empty map
  def getSample(sampleID: String): Map[String,Genotype] = {
    getSample(samplesIndex.getOrElse(sampleID,-1))
 }

  //Returns a Map of Variants -> Genotype for that sample
  //In case of an absent sample, returns an empty map
  def getSample(sampleIndex: Int): Map[String,Genotype] = {

    val sample = mutable.Map[String,Genotype]()

    if(variants.isEmpty){return sample}

    if(sampleIndex < 0) { return sample }

    if(sindices.isEmpty){ buildSampleView() }

    val nextSampleIndex = if(sindices.size > sampleIndex+1) sindices(sampleIndex+1) else s_vindices.size

    Range(sindices(sampleIndex),nextSampleIndex).foreach({
      case i => sample.update(variants(s_vindices(i)), Genotype(s_genotypes(i)))
    })

    return sample
  }

  private def buildSampleView() = {

    //Simple class to aggregate data while taking advantage of the variants been ordered
    class SampleMapBuilder {

      var currVariant = 0
      var nextVariantIndex = if (vindices.size > 1) vindices(1) else Int.MaxValue
      val x = ListBuffer[(Int, Int, Byte)]()

      def add(v_gindex: Int, index: Int, genotype: Byte): SampleMapBuilder = {
        //Update variant if moved to the next variant
        while (index >= nextVariantIndex) {
          currVariant += 1
          nextVariantIndex = if (vindices.size > currVariant+1) vindices(currVariant+1) else Int.MaxValue
        }
        x.+=((v_gindex, currVariant, genotype))
        this
      }

      def getSortedIterator: Iterator[(Int, Int, Byte)] = {
        x.sortWith((left,right) => left._1 < right._1).iterator
      }

    }

    //Build a sample -> variant map
    val sampleMap = v_sindices.zipWithIndex.foldLeft(new SampleMapBuilder())({
      case(acc,(v_gindex,i)) => acc.add(v_gindex,i,v_genotypes(i))
    })

    //Populate Arrays
    var currentSample = 0
    sindices += 0
    sampleMap.getSortedIterator.foreach({
      case (sindex,vindex,genotype) =>
        //Check if sample needs to be added
        while(currentSample != sindex){
          sindices += s_genotypes.size
          currentSample += 1
        }
        s_genotypes += genotype
        s_vindices += vindex
    })
    //Add last samples even if no genotypes.
    while(sindices.size < sampleIDs.size){
      sindices += s_genotypes.size
    }

  }

 def getGenotype(variantID: String, sampleID:String) : Option[Genotype] = {

   val sampleIndex = samplesIndex.getOrElse(sampleID,-1)
   if(sampleIndex < 0){ return None}

   val variantIndex = variantsIndex.getOrElse(variantID,-1)
   if(variantIndex < 0){ return None}

   val nextVariantIndex = if(vindices.size > variantIndex+1) vindices(variantIndex+1) else v_sindices.size

   var i = vindices(variantIndex)

   while(i < nextVariantIndex){
     if(v_sindices(i) == sampleIndex){ return Some(Genotype(v_genotypes(i))) }
     i +=1
   }

   /**Range(vindices(variantIndex),nextVariantIndex).foreach({
     case (i) => if(v_sindices(i) == sampleIndex){ return Some(Genotype(v_genotypes(i))) }
   })**/

   return Some(Genotype(0)) //TODO would be best not to hardcode

  }

 def getAC(variantID: String) : Int ={

   val variantIndex = variantsIndex.getOrElse(variantID,-1)
   if(variantIndex < 0){ return 0}

   val nextVariantIndex = if(vindices.size > variantIndex+1) vindices(variantIndex+1) else v_genotypes.size

   v_genotypes.slice(vindices(variantIndex),nextVariantIndex).foldLeft(0)({
     case (acc, gt) =>
       val genotype = Genotype(gt)
       if(genotype.isHet){acc +1}
       else if(genotype.isHomVar){acc + 2}
       else{acc}
   })

 }

 /**def cumulativeAF: Double = {

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

 }**/

}
