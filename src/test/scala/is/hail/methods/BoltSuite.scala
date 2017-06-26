package is.hail.methods

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.optimize.linear.ConjugateGradient
import breeze.stats.distributions.Gaussian
import is.hail.SparkSuite
import org.testng.annotations.Test
import scala.util.control.Breaks._

class BoltSuite extends SparkSuite {

  @Test def test(): Unit = {

    val X = DenseMatrix(
      (1.0, 0.1, 0.5, 0.2, 0.4),
      (0.3, 0.4, 0.1, 0.1, 0.7),
      (0.9, 0.3, 0.1, 0.8, 0.4))
    val y = DenseVector(0.1, 0.4, 0.5)

    val (sigmaGSq, sigmaESq) = estimateVarianceParameters(X, y)

    println("Final results")
    println("sigmaGSq: " + sigmaGSq)
    println("sigmaESq: " + sigmaESq)
  }

  def estimateVarianceParameters(X: DenseMatrix[Double], y: DenseVector[Double]): (Double, Double) = {

    // Step 1a: Estimate variance parameters.
    // p33 of supplement

    val M = X.cols
    val N = X.rows
    // TODO: check that y has N rows

    val mcTrials = math.max(math.min(4e9 / (N * N), 15), 3).toInt
    val betaRand = DenseMatrix.rand(M, mcTrials, Gaussian(0, math.sqrt(1.0 / M)))
    val eRandUnscaled = DenseMatrix.rand(N, mcTrials, Gaussian(0, 1))

    val maxS = 4 // TODO: 7
    // use 1-indexes to match pseudocode
    val hSq = new Array[Double](maxS + 1)
    val logDelta = new Array[Double](maxS + 1)
    val f = new Array[Double](maxS + 1)
    var sigmaGSq = 0.0
    var sigmaESq = 0.0
    hSq(1) = 0.25
    logDelta(1) = math.log((1 - hSq(1)) / hSq(1))
    val t1 = evalfREML(logDelta(1), X, y, mcTrials, betaRand, eRandUnscaled)
    f(1) = t1._1
    sigmaGSq = t1._2
    sigmaESq = t1._3

    hSq(2) = if (f(1) < 0) 0.125 else 0.5
    logDelta(2) = math.log((1 - hSq(2)) / hSq(2))
    val t2 = evalfREML(logDelta(2), X, y, mcTrials, betaRand, eRandUnscaled)
    f(2) = t2._1
    sigmaGSq = t2._2
    sigmaESq = t2._3

    breakable {
      for (s <- 3 until maxS) {
        logDelta(s) = (logDelta(s - 2) * f(s - 1) - logDelta(s - 1) * f(s - 2)) / (f(s - 1) - f(s - 2))
        if (math.abs(logDelta(s) - logDelta(s - 1)) < 0.01) {
          break
        }
        val ts = evalfREML(logDelta(s), X, y, mcTrials, betaRand, eRandUnscaled)
        f(s) = ts._1
        sigmaGSq = ts._2
        sigmaESq = ts._3
      }
    }

    (sigmaGSq, sigmaESq)
  }

  def evalfREML(logDelta: Double, X: DenseMatrix[Double], y: DenseVector[Double], mcTrials: Int, betaRand: DenseMatrix[Double], eRandUnscaled: DenseMatrix[Double]): (Double, Double, Double)
  = {
    val M = X.cols
    val N = X.rows
    val delta = math.exp(logDelta)
    val sqrtDelta = math.sqrt(delta)

    // TODO: note that we should not compute H, but instead have a custom CG
    // implementation that does the computation using X directly
    val H = (1.0 / M) * (X * X.t) + delta * DenseMatrix.eye[Double](M)

    val cg = new ConjugateGradient[DenseVector[Double], DenseMatrix[Double]](maxIterations = 100)

    val yRand = DenseMatrix.zeros[Double](N, mcTrials)
    val betaHatRand = DenseMatrix.zeros[Double](M, mcTrials)
    val eHatRand = DenseMatrix.zeros[Double](N, mcTrials)
    for (t <- 0 until mcTrials) {
      // build random phenotypes using pre-generated components
      // N.B. := is matrix subset update
      yRand(::, t) := X * betaRand(::, t) + sqrtDelta * eRandUnscaled(::, t)

      // compute H^-1*y_rand[:,t], where H = XX'/M + deltaI
      val HinvYRand = cg.minimize(yRand(::, t), H)

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
    println()
    println("delta: " + delta)

    // compute BLUP estimated SNP effect sizes and residuals for real phenotypes
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

    println()
    println("betaHatRandSumSq: " + betaHatRandSumSq)
    println("eHatRandSumSq: " + eHatRandSumSq)
    println("betaHatDataSumSq: " + betaHatDataSumSq)
    println("eHatDataSumSq: " + eHatDataSumSq)

    val f = math.log((betaHatRandSumSq / eHatRandSumSq) / (betaHatDataSumSq / eHatDataSumSq))
    val sigmaGSq = ((1.0 / N) * y.t * HinvYData).valueAt(0) // convert 1x1 vector to double
    val sigmaESq = delta * sigmaGSq

    println()
    println("f: " + f)
    println("sigmaGSq: " + sigmaGSq)
    println("sigmaESq: " + sigmaESq)

    (f, sigmaGSq, sigmaESq)
  }

  def sq(x: DenseVector[Double]): Double = {
    x.t * x
  }

}
