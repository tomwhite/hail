package org.broadinstitute.hail.methods

import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.variant._

import scala.collection.mutable

class SparseStatsBuilder extends Serializable {
  private val rowsX: mutable.ArrayBuilder.ofInt = new mutable.ArrayBuilder.ofInt()
  private val valsX: mutable.ArrayBuilder.ofDouble = new mutable.ArrayBuilder.ofDouble()
  private var sumX: Int = 0
  private var sumXX: Int = 0
  private var sumXY: Double = 0.0
  private val missingRows: mutable.ArrayBuilder.ofInt = new mutable.ArrayBuilder.ofInt()

  def merge(row: Int, g: Genotype, y: DenseVector[Double]): SparseStatsBuilder = {
    g.call.map(_.gt) match {
      case Some(0) =>
      case Some(1) =>
        rowsX += row
        valsX += 1.0
        sumX += 1
        sumXX += 1
        sumXY += y(row)
      case Some(2) =>
        rowsX += row
        valsX += 2.0
        sumX += 2
        sumXX += 4
        sumXY += 2 * y(row)
      case None =>
        missingRows += row
      case _ => throw new IllegalArgumentException("Genotype value " + g.call.map(_.gt).get + " must be 0, 1, or 2.")
    }
    this
  }

  def merge(that: SparseStatsBuilder): SparseStatsBuilder = {
    rowsX ++= that.rowsX.result()
    valsX ++= that.valsX.result()
    sumX += that.sumX
    sumXX += that.sumXX
    sumXY += that.sumXY
    missingRows ++= that.missingRows.result()

    this
  }

  def stats(y: DenseVector[Double], n: Int): SparseStats = {
    val missingRowsArray = missingRows.result()
    val nMissing = missingRowsArray.size
    val nPresent = n - nMissing

    val meanX = sumX.toDouble / nPresent
    rowsX ++= missingRowsArray
    (0 until nMissing).foreach(_ => valsX += meanX)

    val rowsXArray = rowsX.result()
    val valsXArray = valsX.result()

    //SparseVector constructor expects sorted indices
    val indices = Array.range(0, rowsXArray.size)
    indices.sortBy(i => rowsXArray(i))
    val x = new SparseVector[Double](indices.map(rowsXArray(_)), indices.map(valsXArray(_)), n)
    val xx = sumXX + meanX * meanX * nMissing
    val xy = sumXY + meanX * missingRowsArray.iterator.map(y(_)).sum

    SparseStats(x, xx, xy, nMissing)
  }
}

case class SparseStats(x: SparseVector[Double], xx: Double, xy: Double, nMissing: Int)

object SparseStats {
  def name = "SparseStats"

  def apply(vds: VariantDataset, ped: Pedigree, cov: CovariateData): RDD[(Variant, SparseStats)] = {
    require(ped.trios.forall(_.pheno.isDefined))
    val sampleCovRow = cov.covRowSample.zipWithIndex.toMap

    val n = cov.data.rows
    val k = cov.data.cols
    val d = n - k - 2
    if (d < 1)
      throw new IllegalArgumentException(n + " samples and " + k + " covariates implies " + d + " degrees of freedom.")

    val sc = vds.sparkContext
    val sampleCovRowBc = sc.broadcast(sampleCovRow)
    val samplesWithCovDataBc = sc.broadcast(sampleCovRow.keySet)

    val samplePheno = ped.samplePheno
    val yArray = (0 until n).flatMap(cr => samplePheno(cov.covRowSample(cr)).map(_.toString.toDouble)).toArray
    val y = DenseVector[Double](yArray)
    val yBc = sc.broadcast(y)

    vds
      .filterSamples { case (s, sa) => samplesWithCovDataBc.value.contains(s) }
      .aggregateByVariantWithKeys[SparseStatsBuilder](new SparseStatsBuilder())(
        (ssb, v, s, g) => ssb.merge(sampleCovRowBc.value(s), g, yBc.value),
        (ssb1, ssb2) => ssb1.merge(ssb2))
      .mapValues(_.stats(yBc.value, n))
  }
}
