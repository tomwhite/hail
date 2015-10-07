package org.broadinstitute.k3.stats

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import org.apache.commons.math3.util.CombinatoricsUtils.factorialLog

import org.broadinstitute.k3.Utils._


class LeveneHaldaneSuite extends TestNGSuite {

  def LH(n: Int, nA: Int)(nAB: Int): Double = {
    assert(nA >= 0 && nA <= n)
    if (nAB < 0 || nAB > nA || (nA - nAB) % 2 != 0) 0
    else {
      val nB = 2 * n - nA
      val nAA = (nA - nAB) / 2
      val nBB = (nB - nAB) / 2
      math.exp(nAB * math.log(2) + factorialLog(n) - (factorialLog(nAA) + factorialLog(nAB) + factorialLog(nBB)) - (factorialLog(2 * n) - (factorialLog(nA) + factorialLog(nB))))
    }
  }

  val examples = List((15, 10), (15, 9), (15, 0), (15, 15), (1, 0), (1, 1), (0, 0), (1526, 431), (1526, 430))

  @Test def pmfTest() {

    def test(e: (Int, Int)): Boolean = {
      val (n, nA) = e
      val p0 = LeveneHaldane(n, nA).probability _
      val p1 = LH(n, nA) _
      (-2 to nA + 2).forall(nAB => compareDouble(p0(nAB), p1(nAB)))
    }
    examples foreach { e => assert(test(e)) }
  }

  @Test def modeTest() {

    def test(e: (Int, Int)): Boolean = {
      val (n, nA) = e
      val LH = LeveneHaldane(n, nA)
      compareDouble(LH.probability(LH.mode), (nA % 2 to nA by 2).map(LH.probability).max)
    }
    examples foreach {e => assert(test(e))}
  }

  @Test def meanTest() {

    def test(e: (Int, Int)): Boolean = {
      val (n, nA) = e
      val LH = LeveneHaldane(n, nA)
      compareDouble(LH.getNumericalMean, (LH.getSupportLowerBound to LH.getSupportUpperBound).map(i => i * LH.probability(i)).sum)
    }
    examples foreach {e => assert(test(e))}
  }

  @Test def varianceTest() {

    def test(e: (Int, Int)): Boolean = {
      val (n, nA) = e
      val LH = LeveneHaldane(n, nA)
      compareDouble(LH.getNumericalVariance + LH.getNumericalMean * LH.getNumericalMean, (LH.getSupportLowerBound to LH.getSupportUpperBound).map(i => i * i * LH.probability(i)).sum)
    }
    examples foreach {e => assert(test(e))}
  }

  @Test def exactTestsTest() {

    def test(e: (Int, Int)): Boolean = {
      val (n, nA) = e
      val LH = LeveneHaldane(n, nA)
      (-2 to nA + 2).forall(nAB => (
        compareDouble(LH.leftMidP(nAB) + LH.rightMidP(nAB), 1.0)
          && compareDouble(LH.leftMidP(nAB), 0.5 * LH.probability(nAB) + (0 to nAB - 1).map(LH.probability).sum)
          && compareDouble(LH.exactMidP(nAB), {val p0 = LH.probability(nAB); (0 to nA).map(LH.probability).filter(_ < p0).sum + 0.5 * (0 to nA).map(LH.probability).filter(_ == p0).sum})
        ))
    }
    examples foreach {e => assert(test(e))}
  }

}
