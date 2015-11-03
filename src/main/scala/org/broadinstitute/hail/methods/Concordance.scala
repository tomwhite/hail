package org.broadinstitute.hail.methods

import org.broadinstitute.hail.variant._

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import org.broadinstitute.hail.variant.GenotypeType._

/*object Merge {
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

  def getMergeGenotype(mergeMode:Int=1,gts:(Option[GenotypeType],Option[GenotypeType])):Option[GenotypeType] = {
    mergeMode match {
      case 1 => mergeRule1(gts) // default -- consensus merging
      case 2 => mergeRule2(gts) // vds1 + NoCall --> vds2
      case 3 => mergeRule3(gts) // vds2 + NoCall --> vds1; Not sure I interpreted this correctly
      case 4 => mergeRule4(gts) // vds1
      case 5 => mergeRule5(gts) // vds2
    }
  }

  def mergeRDDs(rdd1: RDD[((Variant, Int), Genotype)],
    rdd2: RDD[((Variant, Int), Genotype)]): RDD[((Variant, Int), (Option[Genotype], Option[Genotype]))] = {
    val ret = rdd1.fullOuterJoin(rdd2)
    ret
  }

  def mergeRDDs2(mergeMode:Int, rdd1: RDD[((Variant, Int), Genotype)],
    rdd2: RDD[((Variant, Int), Genotype)]): RDD[((Variant, Int),Genotype)] = {
    rdd1
      .fullOuterJoin(rdd2)
      .map(x => getMergeGenotype(x,mergeMode))
  }*/

/*  def concordanceFromRDD(rdd: RDD[((Variant, Int), (Option[Genotype], Option[Genotype]))]): ConcordanceTable = {
    rdd
      .fold
      .fold(new ConcordanceTable2)
  }*/

  /*def apply(mergeMode: Int, vds1: VariantSampleMatrix, vds2: VariantSampleMatrix): VariantSampleMatrix = {
    vds1
      .fullOuterJoin(vds2)
      .map(getMergeGenotype(mergeMode,gts))
      .cache()
  }*/
//}

/*class Merge (mode: Int, vds1: VariantSampleMatrix, vds2: VariantSampleMatrix) {
  // Need to make sure sample names are used and not ids when merging
  // 1. Full outer join with default value of NoCall
  //vds1.fullOuterJoin(vds2)
  // 2. Calculate concordance and Apply a given merge mode
  // 3. Output merged RDD

  // 1. Get Unique set of Sample IDs
  val sampleIds = vds1.sampleIds.toSet ++ vds2.sampleIds.toSet

}*/

/*class ConcordanceTable2 {
  val combMap = Map[(GenotypeType, GenotypeType), Int]()
  private val genotypes = Array(HomRef, Het, HomVar, NoCall)
  for (g1 <- genotypes; g2 <- genotypes)
    combMap((g1, g2)) = 0

  def add(gt1: GenotypeType, gt2: GenotypeType): Unit = {
    combMap((gt1, gt2)) = combMap((gt1, gt2)) + 1
  }
  def result: Map[(GenotypeType, GenotypeType), Int] = combMap
}*/

class ConcordanceTable {
  // Class to keep track of the genotype combinations when comparing two datasets

  // possible genotypes
  private val genotypes = Array(HomRef,Het,HomVar,NoCall)

  // get all combinations of genotypes as tuples
  private val genoCombs = for (g1 <- genotypes; g2 <- genotypes) yield (g1,g2)

  //initialize and populate mutable Map which keeps track of number of occurances of genotype combination
  private val table = collection.mutable.Map[(GenotypeType,GenotypeType),Int]()
  for (gc <- genoCombs) table += (gc -> 0)

  def getValue(k:(GenotypeType,GenotypeType)):String =
    if (table.contains(k)) table.get(k).get.toString else "NA"

  def addCount(k:(GenotypeType,GenotypeType),count:Int=1) =
    table(k) += count

  def isGenotypesEqual(k:(GenotypeType,GenotypeType)): Boolean =
    if (k._1 == k._2)
      true
    else
      false

  def isNoCall(k:(GenotypeType,GenotypeType)): Boolean =
    if (k._1 == NoCall || k._2 == NoCall)
      true
    else
      false

  // the reason I made these as methods rather than values is because the table is mutable
  def numMismatches() = table.filterKeys(!isGenotypesEqual(_)).filterKeys(!isNoCall(_)).values.sum
  def numMatches() = table.filterKeys(isGenotypesEqual).filterKeys(!isNoCall(_)).values.sum
  def numNoCall() = table.filterKeys(isNoCall).values.sum
  def numTotal() = table.values.sum

  def calcDiscordance() =
    if (numTotal == 0)
      Float.NaN
    else
      numMismatches().toFloat / (numMatches() + numMismatches()).toFloat

  def calcConcordance(): Float = 1 - calcDiscordance()

  override def toString: String = {
    val typeNames = Map(HomVar -> "HomVar",Het -> "Het",HomRef -> "HomRef",NoCall -> "NoCall")

    def dataLine(gt1:GenotypeType,genotypes:Array[GenotypeType]): Array[String] =
      for (gt2 <- genotypes) yield getValue((gt1,gt2))

    val data:Array[String] = for (gt1 <- genotypes) yield typeNames.get(gt1).get + "\t" + dataLine(gt1,genotypes).mkString("\t")

    " \t" + genotypes.map(x => typeNames.get(x).get).mkString("\t") + "\n" + data.mkString("\n") + "\n"
  }

}
