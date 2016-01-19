package org.broadinstitute.hail.methods

import org.broadinstitute.hail.utils.MultiArray2
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.Utils._

class ConcordanceTable extends Serializable {
  // Class to keep track of the genotype combinations when comparing two datasets

  val table = MultiArray2.fill[Long](5,5)(0)

  private def keyIndex(g: Option[Genotype]): Int = (g.map(_.gtType): @unchecked) match {
    case Some(HomRef) => 0
    case Some(Het) => 1
    case Some(HomVar) => 2
    case Some(NoCall) => 3
    case None => 4
  }

  private def keyIndex(i: Int): Option[GenotypeType] = i match {
    case 0 => Some(HomRef)
    case 1 => Some(Het)
    case 2 => Some(HomVar)
    case 3 => Some(NoCall)
    case 4 => None
  }

  private def isNone(i:Int, j:Int) = i == 4 || j == 4
  private def isGenotypesEqual(i:Int, j:Int) = i == j
  private def isNoCall(i:Int, j:Int) = i == 3 || j == 3
  private def isMatch(i:Int, j:Int) = i == j && i < 3 && j < 3
  private def isMismatch(i: Int, j: Int) = i != j && i < 3 && j < 3

  def addCount(gt1: Option[Genotype], gt2: Option[Genotype],count:Int=1): ConcordanceTable = {
    table(keyIndex(gt1),keyIndex(gt2)) += count
    this
  }

  def getCount(gt1: Option[Genotype],gt2:Option[Genotype]): Long = table(keyIndex(gt1),keyIndex(gt2))

  def merge(other:ConcordanceTable): ConcordanceTable = {
    for ((i,j) <- table.indices) {
      table(i,j) += other.table(i,j)
    }
    this
  }

  def numTotal = {for ((i,j) <- table.indices) yield table(i,j)}.sum
  def numNoCall = {for ((i,j) <- table.indices if isNoCall(i,j)) yield table(i,j)}.sum
  def numMatches = {for ((i,j) <- table.indices if isMatch(i,j)) yield table(i,j)}.sum
  def numMismatches = {for ((i,j) <- table.indices if isMismatch(i,j)) yield table(i,j)}.sum
  def numNone = {for ((i,j) <- table.indices if isNone(i,j)) yield table(i,j)}.sum
  def numCalled = numTotal - numNoCall - numNone

  def concordanceRate = divOption(numMatches,numMatches + numMismatches)
  def discordanceRate = divOption(numMismatches,numMatches + numMismatches)

  def genotypeTypeOrder: Array[Option[GenotypeType]] = Array(Some(HomRef), Some(Het), Some(HomVar), Some(NoCall), None)

  val typeNames = Map(Some(HomRef) -> "HomRef", Some(Het) -> "Het", Some(HomVar) -> "HomVar", Some(NoCall) -> "NoCall", None -> "None")

  def labels: Iterable[String] = for (i <- genotypeTypeOrder; j <- genotypeTypeOrder) yield typeNames.get(i).get + "." + typeNames.get(j).get

  def counts: Iterable[Long] = for ((i,j) <- table.indices) yield table(i,j)

  def results: Map[String,String] = labels.zip(counts.map(_.toString)).toMap + ("concrate" -> concordanceRate.getOrElse("NaN").toString, "n" -> numCalled.toString)

}
