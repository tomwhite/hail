package org.broadinstitute.hail.methods

import org.broadinstitute.hail.SparkSuite
import org.broadinstitute.hail.driver.{FilterVariants, FilterSamples, FilterGenotypes, State}
import org.broadinstitute.hail.utils.TestRDDBuilder
import org.testng.annotations.Test

class FilterSuite extends SparkSuite {

  @Test def filterUtilsTest() {
    import org.broadinstitute.hail.methods.FilterUtils._
    
    def fEmpty[T](fo: FilterOption[T]) {
      assert(fo.ot.isEmpty)
    }
    def fAssert(p: FilterOption[Boolean]) {
      assert(p.ot.getOrElse(false))
    }
    
    fEmpty(new FilterOption(None) fEq new FilterOption(None))
    fEmpty(new FilterOption[Boolean](None) fEq true)

    fAssert(true fEq true)
    fAssert(FilterOption(true) fEq true)
    fAssert(true fEq FilterOption(true))
    fAssert(FilterOption(true) fEq FilterOption(true))

    fEmpty(FilterOption.empty fEq true)
    fEmpty(true fEq FilterOption.empty)
    fEmpty(FilterOption.empty fEq FilterOption(true))
    fEmpty(FilterOption(true) fEq FilterOption.empty)
    fEmpty(FilterOption.empty fEq FilterOption.empty)

    fAssert(Array(1, 2).length fEq 2)

    fAssert(5 fEq 5)
    fAssert(5.0 fEq 5)
    fAssert(5 fEq 5.0)
    fAssert(5.0 fEq 5.0)

    fAssert(4 fLt 5)
    fAssert(FilterOption(4) fLt 5)
    fAssert(4 fLt FilterOption(5))
    fAssert(FilterOption(4) fLt FilterOption(5))

    fAssert(FilterOption(4.0) fLt 5)
    fAssert(4.0 fLt FilterOption(5))
    fAssert(FilterOption(4.0) fLt FilterOption(5))

    fAssert(FilterOption(4) fLt 5.0)
    fAssert(4.0 fLt FilterOption(5.0))
    fAssert(FilterOption(4.0) fLt FilterOption(5.0))
  }

  @Test def test() {
    val vds = LoadVCF(sc, "src/test/resources/sample.vcf")
    val state = State("", sc, sqlContext, vds)

    assert(FilterSamples.run(state, Array("--keep", "-c", "\"^HG\" ~ s.id"))
      .vds.nLocalSamples == 63)

    assert(FilterVariants.run(state, Array("--remove", "-c", "v.start >= 14066228"))
      .vds.nVariants == 173)

    val highGQ = FilterGenotypes.run(state, Array("--remove", "-c", "g.call.exists(c => c.gq < 20)"))
      .vds.expand().collect()

    assert(!highGQ.exists { case (v, s, g) => g.call.exists(c => c.gq < 20) })
    assert(highGQ.count{ case (v, s, g) => g.call.exists(c => c.gq >= 20) } == 31260)

    val vds2 = TestRDDBuilder.buildRDD(1, 1, sc)
    val state2 = State("", sc, sqlContext, vds2)
    val nVariants = vds2.nVariants

    /* forAll { (i: Int, j: Int)
      FilterOption(i < j) == i < FilterOption(j) } */
    /* FilterOption(None) < 5 == FilterOption(None) */
    // keep, remove
    // filter options work in Eval (just some)

    // FilterGenotype(val g: Genotype) extends AnyVal { def gq: FilterOption[Int] = filterOptionFromOption(g.gq) }
    // use FilterGenotype in evaluator
    // test these in both cases


    def eval(cond: String): Option[Boolean] = new Evaluator[FilterOption[Boolean]] (
      "{ import org.broadinstitute.hail.methods.FilterUtils._; import org.broadinstitute.hail.methods.FilterOption; " +
        cond + " }: org.broadinstitute.hail.methods.FilterOption[Boolean]")
      .eval().ot

    assert(eval("true").get)
    assert(eval("new FilterOption(Some(true))").get)
    assert(eval("new FilterOption(None)").isEmpty)
    assert(eval("new FilterOption(None) fEq new FilterOption(None)").isEmpty)
    assert(eval("new FilterOption[Boolean](None) fEq true").isEmpty)


    assert(eval("true fEq true").get)
    assert(eval("new FilterOption(Some(true)) fEq true").get)

    assert(eval("true fEq new FilterOption(Some(true))").get) //FIXME

    assert(eval("new FilterOption(Some(true)) fEq new FilterOption(Some(true))").get)

//    assert(eval("true fEq new FilterOption(None)").isEmpty)
//    assert(eval("new FilterOption(None) fEq true").isEmpty)
//    assert(eval("new FilterOption(None) fEq new FilterOption(Some(true))").isEmpty)
//    assert(eval("new FilterOption(Some(true)) fEq new FilterOption(None)").isEmpty)
//
//    assert(eval("false fEq new FilterOption(None)").isEmpty)
//    assert(eval("true fEq new FilterOption(true)").get)
//    assert(eval("new FilterOption(None) fEq new FilterOption(None)").isEmpty)


//    assert(eval("Array(1,2).size fEq 2"))

    assert(FilterVariants.run(state2, Array("--remove", "-c", "true fEq true"))
      .vds.nVariants == 0)

//    assert(FilterVariants.run(state2, Array("--remove", "-c", "true fEq new FilterOption(Some(true))"))
//      .vds.nVariants == 0)

    assert(FilterVariants.run(state2, Array("--remove", "-c", "true"))
      .vds.nVariants == 0)

    assert(FilterVariants.run(state2, Array("--keep", "-c", "5 fEq 5"))
      .vds.nVariants == 1)

    assert(FilterVariants.run(state2, Array("--remove", "-c", "5.0 fEq 5.0"))
      .vds.nVariants == 0)

    assert(FilterVariants.run(state2, Array("--keep", "-c", "5 == 5.0"))
      .vds.nVariants == 1)

    assert(FilterVariants.run(state2, Array("--remove", "-c", "5.0 fEq 5"))
      .vds.nVariants == 0)

    assert(FilterVariants.run(state2, Array("--remove", "-c", "5 fEq 5.0"))
      .vds.nVariants == 0)
//
//    assert(FilterVariants.run(state2, Array("--remove", "-c", "val a = new FilterOption[Int](Some(4)); val b = new FilterOption[Int](Some(5)); a > b"))
//      .vds.nVariants == nVariants)
//
//    assert(FilterVariants.run(state2, Array("--remove", "-c", "new FilterOption[Int](Some(4)) > 5"))
//      .vds.nVariants == nVariants)


  }
}
