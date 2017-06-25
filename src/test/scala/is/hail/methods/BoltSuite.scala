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
    val M = 5
    val N = 3
    val mcTrials = math.max(math.min(4e9 / (N * N), 15), 3).toInt
    val betaRand = DenseMatrix.rand(M, mcTrials, Gaussian(0, math.sqrt(1.0 / M)))
    val eRandUnscaled = DenseMatrix.rand(N, mcTrials, Gaussian(0, 1))

    val h1Sq = 0.25
    val logDelta1 = math.log((1 - h1Sq) / h1Sq)
    val f1 = evalfREML(logDelta1, X, mcTrials, betaRand, eRandUnscaled)
  }

  def evalfREML(logDelta: Double, X: DenseMatrix[Double], mcTrials: Int, betaRand: DenseMatrix[Double], eRandUnscaled: DenseMatrix[Double]): Double
  = {
    val M = X.cols
    val N = X.rows
    val delta = math.exp(logDelta)
    val sqrtDelta = math.sqrt(delta)

    val yRand = DenseMatrix.zeros[Double](N, mcTrials)
    val betaHatRand = DenseMatrix.zeros[Double](M, mcTrials)
    val eHatRand = DenseMatrix.zeros[Double](N, mcTrials)
    //val Hinv = DenseMatrix.zeros[Double]
    for (t <- 0 until mcTrials) {
      // build random phenotypes using pre-generated components
      // N.B. := is matrix subset update
      yRand(::, t) := X * betaRand(::, t) + sqrtDelta * eRandUnscaled(::, t)
      // compute H^-1*y
      // TODO: note that we should not compute H, but instead have a custom CG
      // implementation that does the computation using X directly
      val H = (1.0 / M) * (X * X.t) + delta * DenseMatrix.eye[Double](M)
      val cg = new ConjugateGradient[DenseVector[Double], DenseMatrix[Double]]()
      val HinvYRand = cg.minimize(yRand(::, t), H)
      println(HinvYRand)
      betaHatRand(::, t) := (1.0 / M) * X.t * HinvYRand
      eHatRand(::, t) := delta * HinvYRand
    }
    println()
    println(yRand)
    println()
    println(betaHatRand)
    println()
    println(eHatRand)
    0.0
  }

}
