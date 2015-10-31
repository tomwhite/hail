package org.broadinstitute.hail.methods

import scala.collection.mutable.Map
import org.broadinstitute.hail.variant.GenotypeType._

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
