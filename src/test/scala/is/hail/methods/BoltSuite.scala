package is.hail.methods

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.optimize.linear.ConjugateGradient
import breeze.stats.distributions.Gaussian
import breeze.stats.mean
import is.hail.SparkSuite
import is.hail.annotations.Annotation
import is.hail.stats.{RegressionUtils, ToNormalizedRowMatrix}
import is.hail.utils._
import is.hail.variant.VariantDataset
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.testng.annotations.Test

import scala.util.control.Breaks._

class BoltSuite extends SparkSuite {

  @Test
  def testBoltExampleData(): Unit = {
    val vds = hc.importPlinkBFile("src/test/resources/bolt-lmm/EUR_subset", quantPheno = true)
    val (nSamples, nVariants) = vds.count()
    assert(nSamples == 379, "nSamples total")
    assert(nVariants == 54051, "nVariants total")

    val removeSamplesPath = "src/test/resources/bolt-lmm/EUR_subset.remove"
    val excludeSnpsPath = "src/test/resources/bolt-lmm/EUR_subset.exclude"
    val vdsRemove = removeSamples(vds, hadoopConf, removeSamplesPath)
    val vdsExclude = excludeSnps(vdsRemove, hadoopConf, excludeSnpsPath)

    val (nSamplesFiltered, nVariantsFiltered) = vdsExclude.count()
    assert(nSamplesFiltered == 373, "nSamples after processing remove file")
    assert(nVariantsFiltered == 48646, "nVariants after processing exclude file")

    val phenotypesPath = "src/test/resources/bolt-lmm/EUR_subset.pheno.covars"
    val vdsPheno = annotatePhenotypes(vdsExclude, phenotypesPath)
    assert(vdsPheno.count()._1 == 369, "nSamples after removing missing phenotypes")

    val (y, _, _) = RegressionUtils.getPhenoCovCompleteSamples(vdsPheno, "sa.pheno", Array.empty[String])
    val ynorm = meanCenterNormalize(y)
    println(y)
    println(ynorm)

    val vdsPhenoRowMatrix = ToNormalizedRowMatrix(vdsPheno)
    val xnorm = toBreeze(vdsPhenoRowMatrix).t
    println(xnorm)

    val (sigmaGSq, sigmaESq) = estimateVarianceParameters(xnorm, ynorm)

    println("Final results")
    println("sigmaGSq: " + sigmaGSq)
    println("sigmaESq: " + sigmaESq)

    val sigma2K = 0.139236 // from running BOLT-LMM, sigma2K seems to be sigma g squared
    // TODO: this passed once, but converges onto different values (unstable)
    assert(math.abs((sigmaGSq - sigma2K)/sigmaGSq) < 0.01, "Should be better than 1%")
  }

  //@Test
  def testBoltExampleDataWithVariantsRemoved(): Unit = {
    val vds = hc.importPlinkBFile("src/test/resources/bolt-lmm/EUR_subset", quantPheno = true)
    val (nSamples, nVariants) = vds.count()
    assert(nSamples == 379, "nSamples total")
    assert(nVariants == 54051, "nVariants total")

    val removeSamplesPath = "src/test/resources/bolt-lmm/EUR_subset.remove"
    val excludeSnpsPath = "src/test/resources/bolt-lmm/EUR_subset.exclude.more"
    val vdsRemove = removeSamples(vds, hadoopConf, removeSamplesPath)

    // TODO: iterate over annotations to get SNP ids
    // tail -n +101 /tmp/snps.txt > src/test/resources/bolt-lmm/EUR_subset.exclude.more
//    import java.io._
//    val pw = new PrintWriter(new File("/tmp/snps.txt"))
//    vdsRemove.variantsAndAnnotations.map {
//      case (_, va) =>
//        va.asInstanceOf[GenericRow].get(0).asInstanceOf[String]
//    }.collect().foreach(pw.println(_))
//    pw.close()

    val vdsExclude = excludeSnps(vdsRemove, hadoopConf, excludeSnpsPath)

    val (nSamplesFiltered, nVariantsFiltered) = vdsExclude.count()
    assert(nSamplesFiltered == 373, "nSamples after processing remove file")
    assert(nVariantsFiltered == 100, "nVariants after processing exclude file")

    val phenotypesPath = "src/test/resources/bolt-lmm/EUR_subset.pheno.covars"
    val vdsPheno = annotatePhenotypes(vdsExclude, phenotypesPath)
    assert(vdsPheno.count()._1 == 369, "nSamples after removing missing phenotypes")

    val (y, _, _) = RegressionUtils.getPhenoCovCompleteSamples(vdsPheno, "sa.pheno", Array.empty[String])
    val ynorm = meanCenterNormalize(y)
    println(y)
    println(ynorm)

    val vdsPhenoRowMatrix = ToNormalizedRowMatrix(vdsPheno)
    val xnorm = toBreeze(vdsPhenoRowMatrix).t
    println(xnorm)

//    val (sigmaGSq, sigmaESq) = estimateVarianceParameters(xnorm, ynorm)
//
//    println("Final results")
//    println("sigmaGSq: " + sigmaGSq)
//    println("sigmaESq: " + sigmaESq)
  }

  def removeSamples(vds: VariantDataset, hadoopConf: Configuration, removeSamplesPath:
    String): VariantDataset = {
    val removedSamples = hadoopConf.readLines(removeSamplesPath)(_.map(_.map(
      _.split("\\s+")(1) // IID
    ).value: Annotation).toSet)
    vds.filterSamplesList(removedSamples, keep = false)
  }

  def excludeSnps(vds: VariantDataset, hadoopConf: Configuration, excludeSnpsPath:
  String): VariantDataset = {
    val excludedSnps = hadoopConf.readLines(excludeSnpsPath)(_.map(_.value: String).toSet)
    vds.filterVariants {
      case (_, va, _) =>
        val va0 = va.asInstanceOf[GenericRow].get(0)
        !excludedSnps.contains(va0.asInstanceOf[String])
    }
  }

  def annotatePhenotypes(vds: VariantDataset, phenotypesPath: String): VariantDataset = {
    val phenotypes = hc.importTable(phenotypesPath, impute = true, separator = " ")
      .keyBy("IID")
    lazy val vdsPheno = vds.annotateSamplesTable(phenotypes, expr = "sa.pheno=table.PHENO")
    vdsPheno.filterSamplesExpr("isDefined(sa.pheno) && sa.pheno != -9") // -9 == missing
  }

  def toBreeze(rowMatrix: RowMatrix): DenseMatrix[Double] = {
    val m = rowMatrix.numRows().toInt
    val n = rowMatrix.numCols().toInt
    val mat = DenseMatrix.zeros[Double](m, n)
    var i = 0
    rowMatrix.rows.collect().foreach { vector =>
      vector.foreachActive { case (j, v) =>
        mat(i, j) = v
      }
      i += 1
    }
    mat
  }

  //@Test
  def test(): Unit = {

    val X = DenseMatrix(
      (1.0, 0.1, 0.5, 0.2, 0.4),
      (0.3, 0.4, 0.1, 0.1, 0.7),
      (0.9, 0.3, 0.1, 0.8, 0.4))
    val y = DenseVector(0.1, 0.4, 0.5)

    val (xnorm, ynorm) = normalizeData(X, y)
    //    println("X: " + X)
    //    println("Xnorm: " + xnorm)
    //    println("y: " + y)
    //    println("y mean centered and normalized: " + ynorm)
    //    println("mean: " + mean(ynorm))
    //    println("var: " + variance(ynorm))
    //    println("norm: " + norm(ynorm))

    val (sigmaGSq, sigmaESq) = estimateVarianceParameters(xnorm, ynorm)

    println("Final results")
    println("sigmaGSq: " + sigmaGSq)
    println("sigmaESq: " + sigmaESq)
  }

  def meanCenterNormalize(v: DenseVector[Double]): DenseVector[Double] = {
    val meanCentered = v - mean(v)
    val norm2 = sq(meanCentered)
    if (norm2 > 0) {
      val scale = math.sqrt(v.length / norm2)
      return meanCentered * scale
    }
    meanCentered
  }

  def variance(v: DenseVector[Double]): Double = {
    val m = mean(v)
    var s = 0.0
    for (i <- 0 until v.length) {
      s += (v(i) - m) * (v(i) - m)
    }
    s / v.length
  }

  def normalizeData(X: DenseMatrix[Double], y: DenseVector[Double]): (DenseMatrix[Double], DenseVector[Double]) = {
    val Xnorm = DenseMatrix.zeros[Double](X.rows, X.cols)
    for (c <- 0 until X.cols) {
      Xnorm(::, c) := meanCenterNormalize(X(::, c))
    }
    (Xnorm, meanCenterNormalize(y))
  }

  def estimateVarianceParameters(X: DenseMatrix[Double], y: DenseVector[Double]): (Double, Double) = {

    // Step 1a: Estimate variance parameters.
    // p33 of supplement

    val M = X.cols
    val N = X.rows
    require(y.length == N, "y must have same number of rows as X has columns (" + N + ")")

    val mcTrials = math.max(math.min(4e9 / (N * N), 15), 3).toInt
    println("Using default number of random trials: " + mcTrials)

    val betaRand = DenseMatrix.rand(M, mcTrials, Gaussian(0, math.sqrt(1.0 / M)))
    val eRandUnscaled = DenseMatrix.rand(N, mcTrials, Gaussian(0, 1))

    val maxS = 7
    // use 1-indexes below to match pseudocode
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
          println("Secant iteration for h2 estimation converged in " + (s - 3) + " steps")
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
    println("Estimating MC scaling f_REML at log(delta) = " + logDelta)

    val M = X.cols
    val N = X.rows
    val delta = math.exp(logDelta)
    val sqrtDelta = math.sqrt(delta)

    // TODO: note that we should not compute H, but instead have a custom CG
    // implementation that does the computation using X directly
    val H = (1.0 / M) * (X * X.t) + delta * DenseMatrix.eye[Double](N)

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
//    println()
//    println("yRand: " + yRand)
//    println()
//    println("betaHatRand: " + betaHatRand)
//    println()
//    println("eHatRand: " + eHatRand)
//    println()
//    println("delta: " + delta)

    // compute BLUP estimated SNP effect sizes and residuals for real phenotypes
    val HinvYData = cg.minimize(y, H)
    val betaHatData = (1.0 / M) * X.t * HinvYData
    val eHatData = delta * HinvYData
//    println()
//    println("y:" + y)
//    println()
//    println("betaHatData: " + betaHatData)
//    println()
//    println("eHatData: " + eHatData)

    // evaluate fREML
    var betaHatRandSumSq = 0.0
    var eHatRandSumSq = 0.0
    for (t <- 0 until mcTrials) {
      betaHatRandSumSq += sq(betaHatRand(::, t))
      eHatRandSumSq += sq(eHatRand(::, t))
    }
    val betaHatDataSumSq = sq(betaHatData)
    val eHatDataSumSq = sq(eHatData)

//    println()
//    println("betaHatRandSumSq: " + betaHatRandSumSq)
//    println("eHatRandSumSq: " + eHatRandSumSq)
//    println("betaHatDataSumSq: " + betaHatDataSumSq)
//    println("eHatDataSumSq: " + eHatDataSumSq)

    val f = math.log((betaHatRandSumSq / eHatRandSumSq) / (betaHatDataSumSq / eHatDataSumSq))
    val sigmaGSq = ((1.0 / N) * y.t * HinvYData).valueAt(0)
    // convert 1x1 vector to double
    val sigmaESq = delta * sigmaGSq

//    println()
//    println("f: " + f)
//    println("sigmaGSq: " + sigmaGSq)
//    println("sigmaESq: " + sigmaESq)

    println("  MCscaling: logDelta = " + logDelta + ", f = " + f)

    (f, sigmaGSq, sigmaESq)
  }

  def sq(x: DenseVector[Double]): Double = {
    x.t * x
  }

}
