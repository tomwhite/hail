package is.hail.methods

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.optimize.linear.ConjugateGradient
import breeze.stats.distributions.Gaussian
import is.hail.SparkSuite
import org.testng.annotations.Test

class BoltSuite extends SparkSuite {

  @Test def test(): Unit = {

    // Step 1a: Estimate variance parameters.
    // p33 of supplement
    val X = DenseMatrix(
      (1.0, 0.1, 0.5, 0.2, 0.4),
      (0.3, 0.4, 0.1, 0.1, 0.7),
      (0.9, 0.3, 0.1, 0.8, 0.4))
    val y = DenseVector(0.1, -0.4, 0.5)
    val M = X.cols // 5
    val N = X.rows // 3
    val mcTrials = math.max(math.min(4e9 / (N * N), 15), 3).toInt
    val betaRand = DenseMatrix.rand(M, mcTrials, Gaussian(0, math.sqrt(1.0 / M)))
    val eRandUnscaled = DenseMatrix.rand(N, mcTrials, Gaussian(0, 1))

    val h1Sq = 0.25
    val logDelta1 = math.log((1 - h1Sq) / h1Sq)
    val f1 = evalfREML(logDelta1, X, y, mcTrials, betaRand, eRandUnscaled)

    val h2Sq = if (f1 < 0) 0.125 else 0.5
    val logDelta2 = math.log((1 - h2Sq) / h2Sq)
    val f2 = evalfREML(logDelta2, X, y, mcTrials, betaRand, eRandUnscaled)


  }

  def evalfREML(logDelta: Double, X: DenseMatrix[Double], y: DenseVector[Double], mcTrials: Int, betaRand: DenseMatrix[Double], eRandUnscaled: DenseMatrix[Double]): Double
  = {
    val M = X.cols
    val N = X.rows
    val delta = math.exp(logDelta)
    val sqrtDelta = math.sqrt(delta)

    // TODO: note that we should not compute H, but instead have a custom CG
    // implementation that does the computation using X directly
    val H = (1.0 / M) * (X * X.t) + delta * DenseMatrix.eye[Double](M)

    val yRand = DenseMatrix.zeros[Double](N, mcTrials)
    val betaHatRand = DenseMatrix.zeros[Double](M, mcTrials)
    val eHatRand = DenseMatrix.zeros[Double](N, mcTrials)
    for (t <- 0 until mcTrials) {
      // build random phenotypes using pre-generated components
      // N.B. := is matrix subset update
      yRand(::, t) := X * betaRand(::, t) + sqrtDelta * eRandUnscaled(::, t)

      // compute H^-1*y_rand[:,t], where H = XX'/M + deltaI
      val cg = new ConjugateGradient[DenseVector[Double], DenseMatrix[Double]]()
      val HinvYRand = cg.minimize(yRand(::, t), H)
      println("HinvYRand: " + HinvYRand)

      // compute BLUP estimated SNP effect sizes and residuals
      betaHatRand(::, t) := (1.0 / M) * X.t * HinvYRand
      eHatRand(::, t) := delta * HinvYRand
    }
    println()
    println("yRand: " + yRand)
    println()
    println("betaHatRand: " + betaHatRand)
    println()
    println("eHatRand: " + eHatRand)

    // compute BLUP estimated SNP effect sizes and residuals for real phenotypes
    val cg = new ConjugateGradient[DenseVector[Double], DenseMatrix[Double]]()
    val HinvYData = cg.minimize(y, H)
    val betaHatData = (1.0 / M) * X.t * HinvYData
    val eHatData = delta * HinvYData
    println()
    println("y:" + y)
    println()
    println("betaHatData: " + betaHatData)
    println()
    println("eHatData: " + eHatData)

    // evaluate fREML
    var betaHatRandSumSq = 0.0
    var eHatRandSumSq = 0.0
    for (t <- 0 until mcTrials) {
      betaHatRandSumSq += sq(betaHatRand(::, t))
      eHatRandSumSq += sq(eHatRand(::, t))
    }
    val betaHatDataSumSq = sq(betaHatData)
    val eHatDataSumSq = sq(eHatData)

    println("betaHatRandSumSq: " + betaHatRandSumSq)
    println("eHatRandSumSq: " + eHatRandSumSq)
    println("betaHatDataSumSq: " + betaHatDataSumSq)
    println("eHatDataSumSq: " + eHatDataSumSq)

    val f = math.log((betaHatRandSumSq / eHatRandSumSq) / (betaHatDataSumSq / eHatDataSumSq))
    println("f: " + f)

    f
  }

  def sq(x: DenseVector[Double]): Double = {
    x.t * x
  }

}
