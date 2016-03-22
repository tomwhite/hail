package org.broadinstitute.hail.stats

import breeze.numerics.sigmoid
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
  */

  @Test def logregTest() = {

    val t = DenseVector(0d, 0d, 1d, 1d, 1d, 1d)

    val X = DenseMatrix(
      (1.0,  0.0, -1.0),
      (1.0,  2.0,  3.0),
      (1.0,  1.0,  5.0),
      (1.0, -2.0,  0.0),
      (1.0, -2.0, -4.0),
      (1.0,  4.0,  3.0))

    println(X)

    def gradient(w: DenseVector[Double]): DenseVector[Double] = {
      val y = sigmoid(X * w)
      X.t * (y :- t)
    }

    def hessian(w: DenseVector[Double]): DenseMatrix[Double] = {
      val y = sigmoid(X * w)
      val R = y :* (1d - y)
      X.t * diag(R) * X
    }

    val nr = new NewtonRaphson(gradient, hessian)

    val w0 = DenseVector(0d, 0d, 0d)
    val wmin = nr.optimize(w0, tolerance = 1.0E-6, iterations = 10)

    assert(D_==(wmin(0),  0.7245, tolerance = 1.0E-3))
    assert(D_==(wmin(1), -0.3586, tolerance = 1.0E-3))
    assert(D_==(wmin(2),  0.1923, tolerance = 1.0E-3))

    /* Testimg against R:
    y0 = c(0, 0, 1, 1, 1, 1)
    c1 = c(0, 2, 1, -2, -2, 4)
    c2 = c(-1, 3, 5, 0, -4, 3)
    logfit <- glm(y0 ~ c1 + c2, family = binomial(link = "logit"))
    summary(logfit)
    */
  }
}
