package org.broadinstitute.k3.methods

import org.broadinstitute.k3.variant._

// FIXME: need to account for all HomRef
object nGenotypePerSample extends SampleMethod[nGenotype] {
  def name = "nGenotypePerSample"

  def apply(vds: VariantDataset): Map[Int, nGenotype] = {
    vds
      .mapValues (g =>
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
      .foldBySample(nGenotype())(_ + _)
  }
}


