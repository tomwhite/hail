package org.broadinstitute.hail.variant

import org.broadinstitute.hail.ByteIterator
import org.broadinstitute.hail.check.Gen
import org.broadinstitute.hail.check.Properties
import org.broadinstitute.hail.check.Prop._
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test
import org.broadinstitute.hail.Utils._

import scala.collection.mutable

object GenotypeSuite {
  val ab = new mutable.ArrayBuilder.ofByte

  def readWriteEqual(v: Variant, g: Genotype): Boolean = {
    ab.clear()

    val gb = new GenotypeBuilder(v.nAlleles)

    gb.set(g)
    gb.write(ab)
    val g2 = Genotype.read(v.nAlleles, new ByteIterator(ab.result()))

    g == g2
  }

  object Spec extends Properties("Genotype") {
    property("readWrite") = forAll[(Variant, Genotype)](Genotype.genVariantGenotype) { case (nAlleles, g) =>
      readWriteEqual(nAlleles, g)
    }

    property("gt") = forAll { g: Genotype =>
      g.gt.isDefined == g.isCalled
    }

    property("gtPairIndex") = forAll(Gen.choose(0, 0x7fff),
      Gen.choose(0, 0x7fff)) { (i: Int, j: Int) =>
      (i <= j) ==> (Genotype.gtPair(Genotype.gtIndex(i, j)) == GTPair(i, j))
    }

    property("gtIndexPair") = forAll(Gen.choose(0, 0x20003fff)) { (i: Int) =>
      val p = Genotype.gtPair(i)

      Genotype.gtIndex(p) == i &&
      Genotype.gtPairSqrt(i) == p &&
      Genotype.gtPairRecursive(i) == p
    }

    property("plEqualsPPwithUnifPrior") = forAll { g: Genotype =>
      if (g.px.isDefined) {
        if (g.isPL) {
          val ppTransformed = g.pp(Option(uniformPriorPhred(g.px.get.length))).get
          val res =  ppTransformed sameElements g.pl().get
          if (!res)
            println(s"g.pp=${ppTransformed.mkString(",")} g.pl=${g.pl().get.mkString(",")}")
          res
        } else if (g.isPP) {
          val plTransformed = g.pl(Option(uniformPriorPhred(g.px.get.length))).get
          val res = plTransformed sameElements g.pp().get
          if (!res)
            println(s"g.pl=${plTransformed.mkString(",")} g.pp=${g.pp().get.mkString(",")}")
          res
        } else
          true // not testing GP here
      } else
        g.pl() == g.pp()
    }

    property("gpEqualsPP") = forAll { g: Genotype =>
      if (g.px.isDefined) {
        if (g.isPP) {
          val ppTransformed = Genotype.phredToLinearScale(g.pp().get)
          val ppTransformed2 = Genotype.phredToLinearScale(g.pp(Option(uniformPriorPhred(g.px.get.length))).get)
          val res = (ppTransformed sameElements g.gp().get) && (ppTransformed2 sameElements g.gp().get)
          if (!res)
            println(s"g.pp=${ppTransformed.mkString(",")} g.gp=${g.gp().get.mkString(",")}")
          res
        } else if (g.isGP) {
          val gpTransformed = Genotype.linearToPhredScale(g.px.get)
          val res = gpTransformed sameElements g.pp().get
          if (!res)
            println(s"g.gp=${gpTransformed.mkString(",")} g.pp=${g.pp().get.mkString(",")}")
          res
        } else
          true // not testing PL here
      } else
        g.gp() == g.pp()
    }

    val compGenPL = for (nAlleles: Int <- Gen.choose(2, 10);
                       g: Genotype <- Genotype.genPL(nAlleles);
                       prior: Array[Int] <- Gen.buildableOfN[Array[Int], Int](triangle(nAlleles), Gen.choose(0, 1000))) yield (g, Genotype.renormPhredScale(prior))

    property("nonUnifPriorPLtoPP") = forAll (compGenPL) {case (g: Genotype, prior: Array[Int]) =>
      if (g.px.isDefined) {
        if (g.isPL) {
          val px2 = g.pp(Option(prior))
          val flags = Genotype.flagSetHasPP(g.flags)
          val g2 = g.copy(px = px2, flags = flags) // initialize as Genotype from PP
          val res = g.pl(Option(prior)).get sameElements g2.pl(Option(prior)).get
          res
        } else
          true //not testing PP or GP here
      } else
        true
    }

    val compGenPP = for (nAlleles: Int <- Gen.choose(2, 10);
                         g: Genotype <- Genotype.genPP(nAlleles);
                         prior: Array[Int] <- Gen.buildableOfN[Array[Int], Int](triangle(nAlleles), Gen.choose(0, 1000))) yield (g, Genotype.renormPhredScale(prior))

    property("nonUnifPriorPPtoPL") = forAll (compGenPP) {case (g: Genotype, prior: Array[Int]) =>
      if (g.px.isDefined) {
        if (g.isPP) {
          val px2 = g.pl(Option(prior))
          val flags = Genotype.flagUnsetHasPP(g.flags)
          val g2 = g.copy(px = px2, flags = flags) // initialize as Genotype from PL
          val res = g.pp(Option(prior)).get sameElements g2.pp(Option(prior)).get
          res
        } else
          true //not testing PL or GP here
      } else
        true
    }
  }

}

class GenotypeSuite extends TestNGSuite {

  import GenotypeSuite._

  val v = Variant("1", 1, "A", "T")

  def testReadWrite(g: Genotype) {
    assert(readWriteEqual(v, g))
  }

  @Test def testGenotype() {
    intercept[IllegalArgumentException] {
      Genotype(Some(-2), Some(Array(2, 0)), Some(2), None)
    }

    val noCall = Genotype(None, Some(Array(2, 0)), Some(2), None)
    val homRef = Genotype(Some(0), Some(Array(10, 0)), Some(10), Some(99), Some(Array(0, 1000, 100)))
    val het = Genotype(Some(1), Some(Array(5, 5)), Some(12), Some(99), Some(Array(100, 0, 1000)))
    val homVar = Genotype(Some(2), Some(Array(2, 10)), Some(12), Some(99), Some(Array(100, 1000, 0)))

    assert(noCall.isNotCalled && !noCall.isCalled && !noCall.isHomRef && !noCall.isHet && !noCall.isHomVar)
    assert(!homRef.isNotCalled && homRef.isCalled && homRef.isHomRef && !homRef.isHet && !homRef.isHomVar)
    assert(!het.isNotCalled && het.isCalled && !het.isHomRef && het.isHet && !het.isHomVar)
    assert(!homVar.isNotCalled && homVar.isCalled && !homVar.isHomRef && !homVar.isHet && homVar.isHomVar)

    assert(noCall.gt.isEmpty)
    assert(homRef.gt.isDefined)
    assert(het.gt.isDefined)
    assert(homVar.gt.isDefined)

    testReadWrite(noCall)
    testReadWrite(homRef)
    testReadWrite(het)
    testReadWrite(homVar)

    assert(Genotype(None, None, None, None).pAB().isEmpty)
    assert(D_==(Genotype(None, Some(Array(0, 0)), Some(0), None, None).pAB().get, 1.0))
    assert(D_==(Genotype(Some(1), Some(Array(16, 16)), Some(33), Some(99), Some(Array(100, 0, 100))).pAB().get, 1.0))
    assert(D_==(Genotype(Some(1), Some(Array(5, 8)), Some(13), Some(99), Some(Array(200, 0, 100))).pAB().get, 0.423950))

    Spec.check(size = 100, count = 100000)
  }
}
