package org.broadinstitute.hail.stats

import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import org.broadinstitute.hail.Utils._
import breeze.linalg._

class NewtonRaphsonSuite extends TestNGSuite {

  /*
  @Test def quadraticTest() = {
    val d = 2

    def gradient(x: DenseVector[Double]): DenseVector[Double] = {
      x
    }

    val h = DenseMatrix.eye[Double](d)

    def hessian(x: DenseVector[Double]): DenseMatrix[Double] = {
      h
    }

    val nr = new NewtonRaphson(gradient, hessian)

    val x0 = DenseVector.fill[Double](d, 10.0)
    val xmin = nr.optimize(x0)

    println(xmin)
  }
  */

  @Test def cubicTest() = {
    val d = 1

    def gradient(x: DenseVector[Double]): DenseVector[Double] = {
      DenseVector(3 * x(0) * x(0) - 1)
    }

    def hessian(x: DenseVector[Double]): DenseMatrix[Double] = {
      new DenseMatrix(1, 1, Array[Double](6 * x(0)))
    }

    val nr = new NewtonRaphson(gradient, hessian)

    val x0 = DenseVector(0.01)
    val xmin = nr.optimize(x0, tolerance = 1.0E-10, iterations = 100)(0)
    val xexact = scala.math.sqrt(1.0 / 3)

    println(xexact)

    assert(D_==(xmin, xexact, 1.0E-3))

    println(xmin)
  }

  @Test def covTest() = {

  }
}
