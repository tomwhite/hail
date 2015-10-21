package org.broadinstitute.k3.methods

import org.broadinstitute.k3.variant.Genotype

// FIXME need to account for all HomRef?
object nCalledPer extends SumMethod {
  def name = "nCalled"

  override def map(g: Genotype) = if (g.isCalled) 1 else 0
}

object nHetPer extends SumMethod {
  def name = "nHet"

  override def map(g: Genotype) = if (g.isHet) 1 else 0
}

// FIXME need to account for all HomRef
object nHomRefPer extends SumMethod {
  def name = "nHomRef"

  override def map(g: Genotype) = if (g.isHomRef) 1 else 0
}

object nHomVarPer extends SumMethod {
  def name = "nHomVar"

  override def map(g: Genotype) = if (g.isHomVar) 1 else 0
}

// FIXME: need to account for all HomRef
object nNonRefPer extends DerivedMethod {
  type T = Int

  def name = "nNonRef"

  override def map(values: MethodValues) =
    values.get(nHetPer) + values.get(nHomVarPer)
}

// FIXME need to account for all HomRef?
object nNotCalledPer extends SumMethod {
  def name = "nNotCalled"

  override def map(g: Genotype) = if (g.isNotCalled) 1 else 0
}

object rHetFrequencyPer extends DerivedMethod {
  type T = Option[Double]

  def name = "rHetFrequency"

  override def map(values: MethodValues) = {
    val nCalled = values.get(nCalledPer)
    val nHet = values.get(nHetPer)
    if (nCalled != 0) Some(nHet.toDouble / nCalled) else None
  }
}

// FIXME: need to account for all HomRef
object rHetHomPer extends DerivedMethod {
  type T = Option[Double]

  def name = "rHetHomVar"

  override def map(values: MethodValues) = {
    val nHomVar = values.get(nHomVarPer)
    val nHet = values.get(nHetPer)
    if (nHomVar != 0) Some(nHet.toDouble / nHomVar) else None
  }
}