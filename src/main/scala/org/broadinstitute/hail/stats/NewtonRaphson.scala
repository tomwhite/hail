package org.broadinstitute.hail.stats

import breeze.linalg._
import org.broadinstitute.hail.Utils._

class NewtonRaphson(gradient: DenseVector[Double] => DenseVector[Double], hessian: DenseVector[Double] => DenseMatrix[Double]) {

  def optimize(x0: DenseVector[Double], tol: Double = 1e-3, iterations: Int = 10): DenseVector[Double] = {
    val x = x0
    val g = gradient(x0)
    val h = hessian(x0)

    for (i <- 0 until iterations) {
      x := x - inv(h) * g
      g := gradient(x)

      if (norm(g) < tol)
        return x

      h := hessian(x)
    }

    fatal("failed to converge")
  }
}
