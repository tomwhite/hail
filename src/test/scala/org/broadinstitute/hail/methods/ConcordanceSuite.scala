package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.utils.TestRDDBuilder.buildRDD
import org.testng.annotations.Test
import scala.math.abs

class ConcordanceSuite extends SparkSuite{
  @Test def test() = {
    val ct = new ConcordanceTable

    ct.addCount((HomRef,HomRef),1000)
    ct.addCount((Het,Het),200)
    ct.addCount((HomVar,HomVar),10)
    ct.addCount((Het,HomRef),20)
    ct.addCount((HomRef,Het),50)
    ct.addCount((NoCall,NoCall))
    ct.addCount((NoCall,HomRef),5)
    ct.addCount((HomRef,NoCall),8)
    ct.addCount((NoCall,Het),30)
    ct.addCount((NoCall,HomVar),15)
    ct.addCount((NoCall,HomVar),9)

    
    assert(ct.numMismatches() == 70)
    assert(ct.numMatches() == 1210)
    assert(ct.numNoCall() == 68)
    assert(ct.numTotal() == 1348)
    assert(closeEnough(ct.calcDiscordance, .0546875))
    assert(abs(ct.calcDiscordance() - 0.0546875) < 0.0001)
    assert(abs(ct.calcConcordance() - 0.9453125) < 0.0001)
    assert(ct.calcConcordance() == 1 - ct.calcDiscordance())
    assert(ct.getValue((HomRef,HomRef)) == "1000")
    assert(ct.toString == " \tHomRef\tHet\tHomVar\tNoCall\nHomRef\t1000\t50\t0\t8\nHet\t20\t200\t0\t0\nHomVar\t0\t0\t10\t0\nNoCall\t5\t30\t24\t1\n")

  }
}
