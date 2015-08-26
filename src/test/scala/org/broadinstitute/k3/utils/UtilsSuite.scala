package org.broadinstitute.k3.utils

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

import org.broadinstitute.k3.Utils._

class UtilsSuite extends TestNGSuite {
  @Test def testCompareDouble() {
    assert(compareDouble(1, 1))
    assert(compareDouble(1, 1 + 1E-7))
    assert(!compareDouble(1, 1 + 1E-5))
    assert(!compareDouble(1E10, 1E10 + 1))
    assert(!compareDouble(1E-10, 2E-10))
    assert(compareDouble(0.0, 0.0))
  }
}
