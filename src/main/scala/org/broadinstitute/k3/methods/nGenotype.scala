package org.broadinstitute.k3.methods

case class nGenotype(nHomRef: Int = 0, nHet: Int = 0, nHomVar: Int = 0, nNotCalled: Int = 0) {
  def nCalled = nHomRef + nHet + nHomVar
  def nTotal = nHomRef + nHet + nHomVar + nNotCalled

  def +(that: nGenotype) = nGenotype(nHomRef + that.nHomRef, nHet + that.nHet, nHomVar + that.nHomVar, nNotCalled + that.nNotCalled)
}
