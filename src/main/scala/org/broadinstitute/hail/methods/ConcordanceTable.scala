package org.broadinstitute.hail.methods

import org.broadinstitute.hail.variant._

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.Utils._

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
    val gt1t = gt1.map(_.gtType)
    val gt2t = gt2.map(_.gtType)

    table((gt1t, gt2t)) += count
    this
  }

  def merge(other:ConcordanceTable): ConcordanceTable = {
    val mergeCT = new ConcordanceTable // group by key, map, join
    // don't create a new table (don't want to duplicate)
    // want to use a function so doesn't matter how table is implemented
    for (gt1 <- possibleTypes) {
      for (gt2 <- possibleTypes) {
        mergeCT.table((gt1,gt2)) += (this.table((gt1,gt2)) + other.table((gt1,gt2)))
      }
    }
    mergeCT
  }

  def isGenotypesEqual(k:(Option[GenotypeType],Option[GenotypeType])): Boolean =
    k._1 == k._2

  def isNoCall(k:(Option[GenotypeType],Option[GenotypeType])): Boolean =
    k._1.contains(NoCall) || k._2.contains(NoCall)


  def isNone(k:(Option[GenotypeType],Option[GenotypeType])): Boolean =
    k._1.isEmpty || k._2.isEmpty

  // the reason I made these as methods rather than values is because the table is mutable
  def numMismatches = table.filterKeys(!isNone(_)).filterKeys(!isGenotypesEqual(_)).filterKeys(!isNoCall(_)).values.sum
  def numMatches = table.filterKeys(isGenotypesEqual).filterKeys(!isNoCall(_)).values.sum
  def numNoCall = table.filterKeys(isNoCall).values.sum
  def numTotal = table.values.sum

  def calcDiscordance =
    divOption(numMismatches,numMatches + numMismatches)

  def calcConcordance: Option[Double] = {
    calcDiscordance match {
        // change to map function
      case Some(value) => Some(1-value)
      case None => None
    }
  }

  def writeConcordance(sep:String="\t"): String = {
    val data = for (i <- possibleTypes; j <- possibleTypes) yield table.get((i,j)).getOrElse("NA")
    val conc = calcConcordance
    conc match { // make string interpolation only!
      case Some(x) => s"%s$sep%.2f$sep%s".format(numTotal,conc.get,data.mkString(sep))
      case None => s"%s$sep%s$sep%s".format(numTotal,"NaN",data.mkString(sep))
    }
  }

  def pretty: String = {

    def dataLine(gt1:Option[GenotypeType],genotypes:Array[Option[GenotypeType]]): Array[String] =
      for (gt2 <- possibleTypes) yield table.get((gt1,gt2)).get.toString

    val data:Array[String] = for (gt1 <- possibleTypes) yield typeNames.get(gt1).get + "\t" + dataLine(gt1,possibleTypes).mkString("\t")

    " \t" + possibleTypes.map(x => typeNames.get(x).get).mkString("\t") + "\n" + data.mkString("\n") + "\n"
  }

}
