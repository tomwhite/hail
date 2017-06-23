package is.hail.methods

import breeze.linalg.DenseMatrix
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
    val yRand = DenseMatrix.zeros[Double](X.rows, mcTrials)
    for (t <- 0 until mcTrials) {
      // build random phenotypes using pre-generated components
      yRand(::, t) := X * betaRand(::, t) + math.sqrt(logDelta) * eRandUnscaled(::, t) // TODO: sqrt(logDelta) or sqrt(delta)
    }
    println(yRand)
    0.0
  }

}
