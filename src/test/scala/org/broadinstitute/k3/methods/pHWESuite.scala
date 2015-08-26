package org.broadinstitute.k3.methods

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

import org.broadinstitute.k3.variant._


class pHWESuite extends TestNGSuite {
  val conf = new SparkConf().setMaster("local").setAppName("test")
  conf.set("spark.sql.parquet.compression.codec", "uncompressed")
  // FIXME KryoSerializer causes jacoco to throw IllegalClassFormatException exception
  // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val sc = new SparkContext(conf)

  @Test def test() {
//    val vds = LoadVCF(sc, "sparky", "src/test/resources/HWE_test.vcf")
//    val pHWE = pHWEPerVariant(vds).map{ case (v, p) => (v.start, p) }.collectAsMap().toMap
//
//    assert(???)

    sc.stop()
  }
}
