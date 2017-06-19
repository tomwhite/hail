package is.hail.methods

import breeze.linalg.DenseMatrix
import breeze.stats.distributions.Gaussian
import is.hail.SparkSuite
import org.testng.annotations.Test

class BoltSuite extends SparkSuite {

  @Test def test(): Unit = {

    // Step 1a: Estimate variance parameters.
    // p33 of supplement
    val M = 5
    val N = 3
    val mcTrials = math.max(math.min(4e9 / (N * N), 15), 3).toInt
    val betaRand = DenseMatrix.rand(M, mcTrials, Gaussian(0, math.sqrt(1.0 / M)))
    val eRandUnscaled = DenseMatrix.rand(N, mcTrials, Gaussian(0, 1))
  }

}
