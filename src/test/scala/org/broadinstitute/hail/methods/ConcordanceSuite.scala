package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.utils.TestRDDBuilder.buildRDD
import org.testng.annotations.Test
import scala.math.abs

class ConcordanceSuite extends SparkSuite{
/* @Test def test() = {
    val ct1 = new ConcordanceTable

    ct1.addCount(HomRef,HomRef,1000)
    ct1.addCount(Het,Het,200)
    ct1.addCount(HomVar,HomVar,10)
    ct1.addCount(Het,HomRef,20)
    ct1.addCount(HomRef,Het,50)
    ct1.addCount(NoCall,NoCall)
    ct1.addCount(NoCall,HomRef,5)
    ct1.addCount(HomRef,NoCall,8)
    ct1.addCount(NoCall,Het,30)
    ct1.addCount(NoCall,HomVar,15)
    ct1.addCount(NoCall,HomVar,9)

    
    assert(ct1.numMismatches() == 70)
    assert(ct1.numMatches() == 1210)
    assert(ct1.numNoCall() == 68)
    assert(ct1.numTotal() == 1348)
    assert(closeEnough(ct1.calcDiscordance, .0546875))
    assert(abs(ct1.calcDiscordance() - 0.0546875) < 0.0001)
    assert(abs(ct1.calcConcordance() - 0.9453125) < 0.0001)
    assert(ct1.calcConcordance() == 1 - ct1.calcDiscordance())
    assert(ct1.table.get((Some(HomRef),Some(HomRef))).get == 1000)
    assert(ct1.toString == " \tHomRef\tHet\tHomVar\tNoCall\nHomRef\t1000\t50\t0\t8\nHet\t20\t200\t0\t0\nHomVar\t0\t0\t10\t0\nNoCall\t5\t30\t24\t1\n")

    val ct2 = new ConcordanceTable

    ct2.addCount(HomRef,HomRef,1501)
    ct2.addCount(Het,Het,201)
    ct2.addCount(Het,Het, 1)
    ct2.addCount(HomVar,HomVar,30)
    ct2.addCount(Het,HomRef,25)
    ct2.addCount(HomRef,Het,55)
    ct2.addCount(NoCall,NoCall)
    ct2.addCount(NoCall,HomRef,16)
    ct2.addCount(HomRef,NoCall,17)
    ct2.addCount(NoCall,Het,20)
    ct2.addCount(NoCall,HomVar,12)
    ct2.addCount(NoCall,HomVar,8)
    ct2.addCount(HomVar,NoCall,5)
    ct2.addCount(Het,HomVar,25)

    assert(ct2.table.get((Some(NoCall),Some(HomVar))).get == 20)


    val ct3 = ct1.merge(ct2)

    assert(ct3.table.get(Some(HomRef),Some(HomRef)).get == 2501)
    assert(ct3.table.get(Some(Het),Some(Het)).get == 402)
    assert(ct3.table.get(Some(NoCall),Some(HomVar)).get == 44)
    assert(ct3.table.get(Some(HomVar),Some(NoCall)).get == 5)
  }*/
}
