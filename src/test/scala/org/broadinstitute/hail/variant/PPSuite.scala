package org.broadinstitute.hail.variant

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.driver.{ExportVCF, FilterGenotypes, ImportVCF, State}
import org.testng.annotations.Test

class PPSuite extends SparkSuite {

  @Test def test1() {
    var s = State(sc, sqlContext, null)
    val testVCF = "src/test/resources/sample.vcf"
    val outVCF = tmpDir.createTempFile("testPriorExpr",".vcf")
    s = ImportVCF.run(s, Array(testVCF))
//    FilterGenotypes.run(s, Array("--remove", "-c", "g.pl[0] != 0"))
//    FilterGenotypes.run(s, Array("--remove", "-c", "g.pl()[0] != 0"))
//    FilterGenotypes.run(s, Array("--remove", "-c", "g.gp[0] != 0"))
//    FilterGenotypes.run(s, Array("--remove", "-c", "g.gp()[0] != 0"))
//    FilterGenotypes.run(s, Array("--remove", "-c", "g.pp[0] != 0"))
//    FilterGenotypes.run(s, Array("--remove", "-c", "g.pp()[0] != 0"))
//    val res = FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(v))[0] != 0"))
//    .vds.rdd.collect()
//    println(res.length)
//    println(res.head)

    val res = FilterGenotypes.run(s, Array("--remove", "-c", "uniformPrior(v)[0] != 0"))
      .vds.rdd.collect()
    println(res.length)
    println(res.head)

    //FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(5))[0] != 0"))
    //FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(v))[0] != 0"))
    //FilterGenotypes.run(s, Array("--remove", "-c", "g.pl(uniformPrior(v, v, v))[0] != 0"))
//    ExportVCF.run(s, Array("-o",outVCF))
  }
}
