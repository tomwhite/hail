package org.broadinstitute.hail.methods

import org.broadinstitute.hail.variant._

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import org.broadinstitute.hail.variant.GenotypeType._

class ConcordanceTable extends Serializable {
  // Class to keep track of the genotype combinations when comparing two datasets

  val table = Map[(Option[GenotypeType], Option[GenotypeType]), Long]()
  val possibleTypes:Array[Option[GenotypeType]] = Array(Some(HomRef), Some(Het), Some(HomVar), Some(NoCall), None)
  val typeNames = Map(Some(HomRef) -> "HomRef",Some(Het) -> "Het", Some(HomVar) -> "HomVar",Some(NoCall) -> "NoCall", None -> "None")
  for (i <- possibleTypes) {
    for (j <- possibleTypes) {
      table((i,j)) = 0
    }
  }

  def addCount(gt1: Option[Genotype], gt2: Option[Genotype],count:Int=1): ConcordanceTable = {
    val gt1t = gt1 match {
      case Some(gt) => Some(gt.gtType)
      case None => None
    }
    val gt2t = gt2 match {
      case Some(gt) => Some(gt.gtType)
      case None => None
    }
    table((gt1t, gt2t)) += count
    this
  }

  def merge(other:ConcordanceTable): ConcordanceTable = {
    val mergeCT = new ConcordanceTable
    for (gt1 <- possibleTypes) {
      for (gt2 <- possibleTypes) {
        mergeCT.table((gt1,gt2)) += (this.table((gt1,gt2)) + other.table((gt1,gt2)))
      }
    }
    mergeCT
  }

  def isGenotypesEqual(k:(Option[GenotypeType],Option[GenotypeType])): Boolean =
    if (k._1 == k._2)
      true
    else
      false

  def isNoCall(k:(Option[GenotypeType],Option[GenotypeType])): Boolean =
    if (k._1 == Some(NoCall) || k._2 == Some(NoCall))
      true
    else
      false

  // the reason I made these as methods rather than values is because the table is mutable
  def numMismatches = table.filterKeys(!isGenotypesEqual(_)).filterKeys(!isNoCall(_)).values.sum
  def numMatches = table.filterKeys(isGenotypesEqual).filterKeys(!isNoCall(_)).values.sum
  def numNoCall = table.filterKeys(isNoCall).values.sum
  def numTotal = table.values.sum

  def calcDiscordance = {
    //println(s"nTot=$numTotal, nMatch=$numMatches, nMis=$numMismatches")
    if ((numMismatches + numMatches) == 0)
      None
    else
      Some(numMismatches.toDouble / (numMatches + numMismatches))
  }

  def calcConcordance: Option[Double] = {
    calcDiscordance match {
      case Some(value) => Some(1-value)
      case None => None
    }
  }

  def writeConcordance(sep:String="\t"): String = {
    val data = for (i <- possibleTypes; j <- possibleTypes) yield table.get((i,j)).getOrElse("NA")
    s"%s$sep%.2f$sep%s".format(numTotal,calcConcordance,data.mkString(sep))
  }

  override def toString: String = {

    def dataLine(gt1:Option[GenotypeType],genotypes:Array[Option[GenotypeType]]): Array[String] =
      for (gt2 <- possibleTypes) yield table.get((gt1,gt2)).get.toString

    val data:Array[String] = for (gt1 <- possibleTypes) yield typeNames.get(gt1).get + "\t" + dataLine(gt1,possibleTypes).mkString("\t")

    " \t" + possibleTypes.map(x => typeNames.get(x).get).mkString("\t") + "\n" + data.mkString("\n") + "\n"
  }

}
