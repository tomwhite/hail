package org.broadinstitute.hail.methods

import org.broadinstitute.hail.MultiArray2
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.Utils._

class ConcordanceTable extends Serializable {
  // Class to keep track of the genotype combinations when comparing two datasets

  val table = MultiArray2.fill[Long](5,5)(0)

  private def keyIndex(g: Option[Genotype]) = g.map(_.gtType) match {
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

  def calcDiscordance = divOption(numMismatches,numMatches + numMismatches)
  def calcConcordance = calcDiscordance.map(x => 1-x)


  def writeConcordance(sep:String="\t"): String = {
    val data = {for ((i,j) <- table.indices) yield table(i,j)}.mkString(sep)
    val conc = calcConcordance.map(x => "%.2f".format(x))
    conc match {
      case Some(x) => s"$numTotal$sep$conc$sep$data"
      case None => s"$numTotal${sep}NaN$sep$data"
    }
  }

}
