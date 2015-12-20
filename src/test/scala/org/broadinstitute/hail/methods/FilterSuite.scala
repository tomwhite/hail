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

    // FilterOption
    val fNone = FilterOption.empty
    val fTrue = FilterOption(true)
    val fFalse = FilterOption(false)

    fAssert(fTrue)
    fEmpty(fNone)

    fAssert(true fEq true)
    fAssert(fTrue fEq true)
    fAssert(true fEq fTrue)
    fAssert(fTrue fEq fTrue)

    fEmpty(fNone fEq true)
    fEmpty(true fEq fNone)
    fEmpty(fNone fEq fTrue)
    fEmpty(fTrue fEq fNone)
    fEmpty(fNone fEq fNone)

    fAssert(true nfEq false)
    fAssert(fTrue nfEq false)
    fAssert(true nfEq fFalse)
    fAssert(fTrue nfEq fFalse)

    // FilterOptionBoolean
    fAssert(fTrue fAnd true)
    fAssert(true fAnd fTrue)
    fAssert(fTrue fAnd fTrue)

    fAssert(fTrue fOr false)
    fAssert(true fOr fFalse)
    fAssert(fTrue fOr fFalse)

    fEmpty(fNone fAnd true)
    fEmpty(true fAnd fNone)
    fEmpty(fNone fAnd fTrue)
    fEmpty(fTrue fAnd fNone)
    fEmpty(fNone fAnd fNone)

    fAssert(!fFalse)
    fEmpty(!fNone)

    // FilterOptionString
    val fString1 = FilterOption("1")
    val fString2 = FilterOption("2.0")

    val fChar = FilterOption('1')

    val fMinusOne = FilterOption(-1)
    val fZero = FilterOption(0)
    val fOne = FilterOption(1)
    val fTwo = FilterOption(2)

    fAssert(fString1(0) fEq '1')
    fAssert(fString1(0) fEq fChar)
    fEmpty(fString1(0) fEq fNone)

    fAssert(fString1.length fEq 1)
    fAssert(fString1.length fEq fOne)
    fEmpty(fString1.length fEq fNone)

    fAssert(fString1 fConcat "2.0" fEq "12.0")
    fAssert("1" fConcat fString2 fEq "12.0")
    fAssert("12.0" fEq ("1" fConcat fString2))
    fAssert("12.0" fEq (fString1 fConcat "2.0"))
    fAssert(fString1 fConcat fString2 fEq "12.0")
    fAssert("12.0" fEq (fString1 fConcat fString2))

    fEmpty(fString1 fConcat fNone fEq "12.0")
    fEmpty(fNone fConcat fString2 fEq "12.0")
    fEmpty("1" fConcat fNone fEq "12.0")
    fEmpty(fNone fConcat "2.0" fEq "12.0")
    fEmpty(fString1 fConcat fString2 fEq fNone)

    fAssert(fString1.toInt fEq 1)
    fAssert(fString2.toDouble fEq 2.0)

    fEmpty(fNone.toInt fEq 1)

    fAssert((fString1.toInt fPlus fString1.toInt).toDouble fEq fString2.toDouble)

    // FilterOptionArray
    val fArray = FilterOption(Array(1, 2))

    fAssert(fArray(0) fEq 1)
    fAssert(fArray(0) fEq fOne)
    fEmpty(fArray(0) fEq fNone)

    fAssert(fArray.size fEq fTwo)
    fAssert(fArray.size fEq 2)
    fEmpty(fArray.size fEq fNone)

    // FilterOptionInt
    val fIntNone = new FilterOption[Int](None) // FIXME: using fNone in place of fIntNone or fDoubleNone does not work

    fAssert(0 fEq 0)
    fAssert(fZero fEq 0)
    fAssert(0 fEq fZero)
    fEmpty(0 fEq fNone)
    fEmpty(fZero fEq fNone)

    //fAssert(1 fPlus 1 fEq 2) // FIXME: overloading issue with 1 converting to Double, Float, Long as well
    //fAssert(fOne fPlus 1 fEq fTwo)
    fAssert(1 fPlus fOne fEq fTwo)
    fAssert(fOne fPlus fOne fEq fTwo)
    //fAssert(fTwo fEq (fOne fPlus 1))
    fAssert(fTwo fEq (1 fPlus fOne))
    fAssert(fTwo fEq (fOne fPlus fOne))

    //fEmpty(fIntNone fPlus 1 fEq fTwo)
    fEmpty(1 fPlus fIntNone fEq fTwo)
    fEmpty(fIntNone fPlus fOne fEq fTwo)
    fEmpty(fOne fPlus fIntNone fEq fTwo)
    fEmpty(fOne fPlus fOne fEq fNone) // Note that fNone works here, as well as fIntNone

    //fAssert(1 fDiv 2 fEq 0)
    //fAssert(fOne fDiv 2 fEq 0)
    fAssert(1 fDiv fTwo fEq 0)
    //fAssert(1 fDiv 2 fEq fZero)

    fAssert(-fMinusOne fEq fOne)
    fEmpty(-fIntNone fEq fOne)

    fAssert(fMinusOne.fAbs fEq fOne)
    fEmpty(fIntNone.fAbs fEq fOne)

    fAssert(fOne.fSignum fEq fOne)
    fAssert(fMinusOne.fSignum fEq fMinusOne)
    fEmpty(fIntNone.fSignum fEq fOne)

    fAssert(fZero fMax 1 fEq fOne)
    fAssert(0 fMax fOne fEq fOne)
    fAssert(fZero fPlus fOne fEq fOne)
    fAssert(fOne fEq (fZero fMax 1))
    fAssert(fOne fEq (0 fPlus fOne))
    fAssert(fOne fEq (fZero fMax fOne))

    fEmpty(fIntNone fMax 1 fEq fOne)
    fEmpty(0 fMax fIntNone fEq fOne)
    fEmpty(fIntNone fMax fOne fEq fOne)
    fEmpty(fZero fMax fIntNone fEq fOne)
    fEmpty(fZero fMax fOne fEq fNone)

    fAssert(fZero fLt fOne)
    fEmpty(fIntNone fLt fOne)
    fEmpty(fZero fLt fIntNone)

    // FilterOptionDouble

    val fDoubleNone = new FilterOption[Double](None)

    val fDoubleMinusOne = FilterOption(-1.0)
    val fDoubleZero = FilterOption(0.0)
    val fDoubleHalf = FilterOption(0.5)
    val fDoubleOne = FilterOption(1.0)
    val fDoubleTwo = FilterOption(2.0)

    fAssert(0.0 fEq 0.0)
    fAssert(fDoubleZero fEq 0.0)
    fAssert(0.0 fEq fDoubleZero)
    fEmpty(fDoubleZero fEq fNone)

    fAssert(1.0 fPlus 1.0 fEq fDoubleTwo)
    fAssert(fDoubleOne fPlus 1.0 fEq fDoubleTwo)
    fAssert(1.0 fPlus fDoubleOne fEq fDoubleTwo)
    fAssert(fDoubleOne fPlus fDoubleOne fEq fDoubleTwo)
    fAssert(fDoubleTwo fEq (fDoubleOne fPlus 1.0))
    fAssert(fDoubleTwo fEq (1.0 fPlus fDoubleOne))
    fAssert(fDoubleTwo fEq (fDoubleOne fPlus fDoubleOne))

    //fEmpty(fDoubleNone fPlus 1 fEq fDoubleTwo)
    fEmpty(1.0 fPlus fDoubleNone fEq fDoubleTwo)
    fEmpty(fDoubleNone fPlus fDoubleOne fEq fDoubleTwo)
    fEmpty(fDoubleOne fPlus fDoubleNone fEq fDoubleTwo)
    fEmpty(fDoubleOne fPlus fDoubleOne fEq fNone)

    fAssert(1.0 fDiv 2.0 fEq 0.5)
    fAssert(fDoubleOne fDiv 2.0 fEq 0.5)
    fAssert(1.0 fDiv fDoubleTwo fEq 0.5)
    fAssert(1.0 fDiv 2.0 fEq fDoubleHalf)

    fAssert(-fDoubleMinusOne fEq fDoubleOne)
    fEmpty(-fDoubleNone fEq fDoubleOne)

    fAssert(fDoubleMinusOne.fAbs fEq fDoubleOne)
    fEmpty(fDoubleNone.fAbs fEq fDoubleOne)

    fAssert(fDoubleOne.fSignum fEq fOne)
    fAssert(fDoubleMinusOne.fSignum fEq fMinusOne)
    fEmpty(fDoubleNone.fSignum fEq fOne)

    fAssert(fDoubleZero fMax 1.0 fEq fDoubleOne)
    fAssert(0.0 fMax fDoubleOne fEq fDoubleOne)
    fAssert(fDoubleZero fPlus fDoubleOne fEq fDoubleOne)
    fAssert(fDoubleOne fEq (fDoubleZero fMax 1.0))
    fAssert(fDoubleOne fEq (0.0 fPlus fDoubleOne))
    fAssert(fDoubleOne fEq (fDoubleZero fMax fDoubleOne))

    fEmpty(fDoubleNone fMax 1.0 fEq fDoubleOne)
    fEmpty(0.0 fMax fDoubleNone fEq fDoubleOne)
    fEmpty(fDoubleNone fMax fDoubleOne fEq fDoubleOne)
    fEmpty(fDoubleZero fMax fDoubleNone fEq fDoubleOne)
    fEmpty(fDoubleZero fMax fDoubleOne fEq fNone)

    fAssert(fDoubleZero fLt fDoubleOne)
    fEmpty(fDoubleNone fLt fDoubleOne)
    fEmpty(fDoubleZero fLt fDoubleNone)

    // FilterOptionInt and FilterOption Double

    fAssert(0 fEq 0.0)
    fAssert(0.0 fEq 0)
    fAssert(0 fEq fDoubleZero)
    fAssert(0.0 fEq fZero)
    fAssert(fDoubleZero fEq 0)
    fAssert(fZero fEq 0.0)
    fAssert(fTwo fEq fDoubleTwo)
    fAssert(fDoubleTwo fEq fTwo)

    fAssert(fOne fPlus 1.0 fEq fTwo)
    fAssert(1.0 fPlus fOne fEq fTwo)
    fAssert(fOne fPlus 1.0 fEq fDoubleTwo)
    fAssert(1.0 fPlus fOne fEq fDoubleTwo)
    fAssert(fDoubleOne fPlus fOne fEq fTwo)
    fAssert(fOne fPlus fDoubleOne fEq fTwo)
    fAssert(fDoubleOne fPlus fOne fEq fDoubleTwo)
    fAssert(fOne fPlus fDoubleOne fEq fDoubleTwo)

    fAssert(1 fDiv 2.toDouble fEq .5)
    fAssert(1 fDiv 2.toDouble fEq fDoubleHalf)

    //fAssert(1.toDouble fDiv 2 fEq .5)
    //fAssert(1.toDouble fDiv 2 fEq fDoubleHalf)

    fAssert(1 fDiv fTwo.toDouble fEq .5)
    fAssert(1.toDouble fDiv fTwo fEq .5)

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

    /*
    forAll { (i: Int, j: Int)
    FilterOption(i < j) == i < FilterOption(j) }
    FilterOption(None) < 5 == FilterOption(None)
    keep, remove
    filter options work in Eval (just some)

    FilterGenotype(val g: Genotype) extends AnyVal { def gq: FilterOption[Int] = filterOptionFromOption(g.gq) }
    use FilterGenotype in evaluator
    test these in both cases
    */

    //val vds2 = TestRDDBuilder.buildRDD(1, 1, sc)
    //val state2 = State("", sc, sqlContext, vds2)
    //val nVariants = vds2.nVariants
  }
}
