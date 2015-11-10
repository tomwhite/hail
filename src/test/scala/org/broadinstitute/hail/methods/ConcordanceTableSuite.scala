package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.variant.GenotypeType._
import org.broadinstitute.hail.variant.Genotype
import org.testng.annotations.Test

class ConcordanceTableSuite extends SparkSuite{
 @Test def test() = {
    val ct1 = new ConcordanceTable

    ct1.addCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0))),1000)
    ct1.addCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))),200)
    ct1.addCount(Some(Genotype(2,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),10)
    ct1.addCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0))),20)
    ct1.addCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))),50)
    ct1.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(-1,(0,0),0,(0,0,0))))
    ct1.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0))),5)
    ct1.addCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(-1,(0,0),0,(0,0,0))),8)
    ct1.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))),30)
    ct1.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),15)
    ct1.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),9)

    
    assert(ct1.numMismatches == 70)
    assert(ct1.numMatches == 1210)
    assert(ct1.numNoCall == 68)
    assert(ct1.numTotal == 1348)
    assert(optionCloseEnough(ct1.calcDiscordance,Some(0.0546875)))
    assert(optionCloseEnough(ct1.calcConcordance,Some(0.9453125)))
    assert(ct1.getCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0)))) == 1000)

    val ct2 = new ConcordanceTable

    ct2.addCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0))),1501)
    ct2.addCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))),201)
    ct2.addCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))), 1)
    ct2.addCount(Some(Genotype(2,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),30)
    ct2.addCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0))),25)
    ct2.addCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))),55)
    ct2.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(-1,(0,0),0,(0,0,0))))
    ct2.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0))),16)
    ct2.addCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(-1,(0,0),0,(0,0,0))),17)
    ct2.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0))),20)
    ct2.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),12)
    ct2.addCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),8)
    ct2.addCount(Some(Genotype(2,(0,0),0,(0,0,0))),Some(Genotype(-1,(0,0),0,(0,0,0))),5)
    ct2.addCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0))),25)

    assert(ct2.getCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0)))) == 20)


    val ct3 = ct1.merge(ct2)

    assert(ct3.getCount(Some(Genotype(0,(0,0),0,(0,0,0))),Some(Genotype(0,(0,0),0,(0,0,0)))) == 2501)
    assert(ct3.getCount(Some(Genotype(1,(0,0),0,(0,0,0))),Some(Genotype(1,(0,0),0,(0,0,0)))) == 402)
    assert(ct3.getCount(Some(Genotype(-1,(0,0),0,(0,0,0))),Some(Genotype(2,(0,0),0,(0,0,0)))) == 44)
    assert(ct3.getCount(Some(Genotype(2,(0,0),0,(0,0,0))),Some(Genotype(-1,(0,0),0,(0,0,0)))) == 5)
  }
}
