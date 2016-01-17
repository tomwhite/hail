package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.driver.VariantEffectAnnotator._
import org.testng.annotations.Test

class VEPSuite extends SparkSuite {
  
  @Test def test1() {
    val oldVds=LoadVCF(sc, "src/test/resources/sample.vcf")
    oldVds.rdd.cache
    val newVds = annotatePartitions(
      oldVds,
      newOptions
    )
    
    assertResult(oldVds.rdd.count)(newVds.rdd.count)

    val (oldVar, oldAnnot, oldGeno) = oldVds.rdd.first
    val (newVar, newAnnot, newGeno) = newVds.rdd.first

    assertResult(oldVar)(newVar)
    assertResult(Set("info"))(oldAnnot.maps.keys)
    assertResult(Set("info","vep"))(newAnnot.maps.keys)
    
    
  }
}
