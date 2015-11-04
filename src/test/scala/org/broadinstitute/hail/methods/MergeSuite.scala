package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test

class MergeSuite extends SparkSuite {
  @Test def test() = {

    val rdd1 = TestRDDBuilder.buildRDD(5, 2, sc, "tuple")
    val rdd2 = TestRDDBuilder.buildRDD(5, 2, sc, "tuple")
//    val merge = new Merge(rdd1,rdd2)

    val mergedVds = Merge(rdd1, rdd2)
    mergedVds.collect().foreach {
      case ((v,s), (g1, g2)) =>
        val g1s = g1 match {
          case Some(gt) => gt.gtString(v)
          case None => "-/-"
        }
        val g2s = g2 match {
          case Some(gt) => gt.gtString(v)
          case None => "-/-"
        }
      println("%s\t%s\t%s\t%s".format(v.start, s, g1s, g2s))
    }

//    println(merge.toString)
  }
}