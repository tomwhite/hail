package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test
import org.broadinstitute.hail.Utils._

class MergeSuite extends SparkSuite {
  @Test def test() = {

    val gt1 = Array(
      Array(0,1,2,1,0),
      Array(-1,1,0,1,0),
      Array(2,1,1,1,2),
      Array(2,-1,1,-1,0),
      Array(0,1,2,2,-1))

    val gt2 = Array(
      Array(2,0,2,1,0),
      Array(2,-1,0,-1,0),
      Array(0,2,1,-1,2),
      Array(-1,-1,-1,0,-1),
      Array(1,1,1,1,1))

    val trueSampleConc = Map(0 -> 0.0, 1 -> 0.3333, 2 -> 0.75, 3 -> 0.5, 4 -> 1.0)
    val rdd1 = TestRDDBuilder.buildRDD(5, 5, sc, "tuple", gtArray = Some(gt1))
    val rdd2 = TestRDDBuilder.buildRDD(5, 5, sc, "tuple", gtArray = Some(gt2))

    val mergedVds = Merge(rdd1, rdd2)
    val sampleConcordance = mergedVds.sampleConcordance.collectAsMap()
    sampleConcordance.foreach{case(k,v) => assert(closeEnough(trueSampleConc.get(k).get,v.calcConcordance))}

    println(mergedVds.writeSampleConcordance("\t"))
    println(mergedVds.writeVariantConcordance("\t"))

/*    mergedVds.collect().foreach {
      case ((v,s), (g1, g2)) =>
        val g1s = g1 match {
          case Some(gt) => gt.gtString(v)
          case None => "-/-"
        }
        val g2s = g2 match {
          case Some(gt) => gt.gtString(v)
          case None => "-/-"
        }
      //println("%s\t%s\t%s\t%s".format(v.start, s, g1s, g2s))
    }*/

/*    val conc = Concordance
    println(conc.writeSampleConcordance(mergedVds,sep=","))
    println(conc.writeVariantConcordance(mergedVds,sep=","))*/
  }
}