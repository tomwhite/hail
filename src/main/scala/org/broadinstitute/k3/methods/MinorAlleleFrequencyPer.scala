package org.broadinstitute.k3.methods

import scala.collection.mutable

object minorAlleleFrequencyPer extends DerivedMethod {

  type T = Double

  def name = "MAF"

  override def emit(values: MethodValues, b: mutable.ArrayBuilder[Any]): Unit = {
    val nHomRef = values.get(nHomRefPer)
    val nHet = values.get(nHetPer)
    val nHomVar = values.get(nHomVarPer)

    val refAlleles = nHomRef * 2 + nHet
    val altAlleles = nHomVar * 2 + nHet
    val maf = altAlleles.toDouble / (refAlleles + altAlleles).toDouble
    b += maf
  }
}
