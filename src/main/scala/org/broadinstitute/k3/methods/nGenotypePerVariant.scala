package org.broadinstitute.k3.methods

import org.apache.spark.rdd.RDD
import org.broadinstitute.k3.variant._

object nGenotypePerVariant extends VariantMethod[nGenotype] {
  def name = "nGenotypePerVariant"

  def apply(vds: VariantDataset): RDD[(Variant, nGenotype)] = {
    vds
      .mapValues(g =>
      if (g.isHomRef)
        nGenotype(nHomRef = 1)
      else if (g.isHet)
        nGenotype(nHet = 1)
      else if (g.isHomVar)
        nGenotype(nHomVar = 1)
      else {
        assert(g.isNotCalled)
        nGenotype(nNotCalled = 1)
      })
      .reduceByVariant(_ + _)
  }
}

