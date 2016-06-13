package org.broadinstitute.hail.variant

import java.util

import org.apache.commons.lang3.builder.HashCodeBuilder
import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.spark.sql.types._
import org.broadinstitute.hail.ByteIterator
import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.driver.HailConfiguration
import org.broadinstitute.hail.io.gen.GenUtils._
import org.broadinstitute.hail.check.{Arbitrary, Gen}
import org.json4s._

import scala.collection.mutable
import scala.language.implicitConversions

object GenotypeType extends Enumeration {
  type GenotypeType = Value
  val HomRef = Value(0)
  val Het = Value(1)
  val HomVar = Value(2)
  val NoCall = Value(-1)
}

import org.broadinstitute.hail.variant.GenotypeType.GenotypeType

object GTPair {
  def apply(j: Int, k: Int): GTPair = {
    require(j >= 0 && j <= 0xffff, "GTPair invalid j value")
    require(k >= 0 && k <= 0xffff, "GTPair invalid k value")
    new GTPair(j | (k << 16))
  }
}

class GTPair(val p: Int) extends AnyVal {
  def j: Int = p & 0xffff

  def k: Int = (p >> 16) & 0xffff

  def nNonRefAlleles: Int =
    (if (j != 0) 1 else 0) + (if (k != 0) 1 else 0)
}

object Foo {
  def f[T](i: Iterable[T], j: Iterable[T]) = i.zip(j)
}

class Genotype(private val _gt: Int,
               private val _ad: Array[Int],
               private val _dp: Int,
               private val _gq: Int,
               private val _px: Array[Int],
               private val _flags: Int) extends Serializable {

  import Genotype._

  require(_gt >= -1, s"invalid _gt value: ${_gt}")
  require(_dp >= -1, s"invalid _dp value: ${_dp}")

  def fakeRef = (flags & Genotype.flagFakeRefBit) != 0
  def isGP = (flags & Genotype.flagHasGPBit) != 0
  def isPP = (flags & Genotype.flagHasPPBit) != 0
  def isPL = !isGP && !isPP

  def check(nAlleles: Int) {
    val nGenotypes = triangle(nAlleles)
    assert(gt.forall(i => i >= 0 && i < nGenotypes))
    assert(ad.forall(a => a.length == nAlleles))
    assert(px.forall(a => a.length == nGenotypes))
  }

  def copy(gt: Option[Int] = this.gt,
           ad: Option[Array[Int]] = this.ad,
           dp: Option[Int] = this.dp,
           gq: Option[Int] = this.gq,
           px: Option[Array[Int]] = this.px,
           flags: Int = this.flags): Genotype = Genotype(gt, ad, dp, gq, px, flags)

  override def equals(that: Any): Boolean = that match {
    case g: Genotype =>
        _gt == g._gt &&
        ((_ad == null && g._ad == null)
          || (_ad != null && g._ad != null && _ad.sameElements(g._ad))) &&
        _dp == g._dp &&
        _gq == g._gq &&
        isGP == g.isGP &&
        isPP == g.isPP &&
        ((!isGP &&  ((_px == null && g._px == null) || (_px != null && g._px != null && _px.sameElements(g._px)))) ||
          (isGP && ((gp().isEmpty && g.gp().isEmpty) || (gp().isDefined && g.gp().isDefined && gp().get.zip(g.gp().get).forall{case (d1,d2) => math.abs(d1 - d2) <= 3.0e-4}))))

    case _ => false
  }

  override def hashCode: Int =
    new HashCodeBuilder(43, 19)
      .append(_gt)
      .append(util.Arrays.hashCode(_ad))
      .append(_dp)
      .append(_gq)
      .append(util.Arrays.hashCode(_px))
      .append(fakeRef)
      .toHashCode

  def gt: Option[Int] =
    if (_gt >= 0)
      Some(_gt)
    else
      None

  def ad: Option[Array[Int]] = Option(_ad)

  def dp: Option[Int] =
    if (_dp >= 0)
      Some(_dp)
    else
      None

  def od: Option[Int] =
    if (_dp >= 0 && _ad != null)
      Some(_dp - _ad.sum)
    else
      None

  def gq: Option[Int] =
    if (_gq >= 0 && !isGP)
      Some(_gq)
    else
      None

  def px: Option[Array[Int]] = if (_px == null) None else Option(_px)

  def pl (prior: Option[Array[Int]] = None): Option[Array[Int]] = {
    if (_px == null)
      None
    else if (isPL)
      Option(_px)
    else if (prior.isDefined) {
      require(prior.get.length == _px.length)
      if (isPP)
        Option(renormPhredScale(_px.zip(prior.get).map { case (i1, i2) => i1 - i2 }))
      else if (isGP)
        Option(renormPhredScale(linearToPhredScale(_px).zip(prior.get).map { case (i1, i2) => i1 - i2 }))
      else
        throw new UnsupportedOperationException
    } else
      None
  }

  def pp (prior: Option[Array[Int]] = None): Option[Array[Int]] = {
    if (_px == null)
      None
    else if (isPP)
      Option(_px)
    else if (isGP)
      Option(renormPhredScale(linearToPhredScale(_px)))
    else if (prior.isDefined) {
      require(prior.get.length == _px.length)
      if (isPL)
        Option(renormPhredScale(_px.zip(prior.get).map { case (i1, i2) => i1 + i2 }))
      else
        throw new UnsupportedOperationException
    } else
      None
  }

  def gp (prior: Option[Array[Int]] = None): Option[Array[Double]] = {
    if (_px == null)
      None
    else if (isGP)
      Option(_px.map{ _ / 32768.0 })
    else if (isPP)
      Option(phredToLinearScale(_px))
    else if (prior.isDefined) {
      require(prior.get.length == _px.length)
      if (isPL)
        Option(phredToLinearScale(_px.zip(prior.get).map { case (i1, i2) => i1 + i2 }))
      else
        throw new UnsupportedOperationException
    } else
      None
  }

  def flags: Int = _flags

  def isHomRef: Boolean = Genotype.isHomRef(_gt)

  def isHet: Boolean = Genotype.isHet(_gt)

  def isHomVar: Boolean = Genotype.isHomVar(_gt)

  def isCalledNonRef: Boolean = Genotype.isCalledNonRef(_gt)

  def isHetNonRef: Boolean = Genotype.isHetNonRef(_gt)

  def isHetRef: Boolean = Genotype.isHetRef(_gt)

  def isNotCalled: Boolean = Genotype.isNotCalled(_gt)

  def isCalled: Boolean = Genotype.isCalled(_gt)

  def gtType: GenotypeType = Genotype.gtType(_gt)

  def nNonRefAlleles: Option[Int] = Genotype.nNonRefAlleles(_gt)

  def toString(exportPL: Boolean = false, exportPP: Boolean = false,
               exportGP: Boolean = false, prior: Option[Array[Int]] = None): String = {
    val b = new StringBuilder

    b.append(gt.map { gt =>
      val p = Genotype.gtPair(gt)
      s"${p.j}/${p.k}"
    }.getOrElse("./."))
    b += ':'
    b.append(ad.map(_.mkString(",")).getOrElse("."))
    b += ':'
    b.append(dp.map(_.toString).getOrElse("."))
    b += ':'
    b.append(gq.map(_.toString).getOrElse("."))

    if (exportPL) {
      b += ':'
      b.append(pl(prior).map(_.mkString(",")).getOrElse("."))
    }
    if (exportPP) {
      b += ':'
      b.append(pp(prior).map(_.mkString(",")).getOrElse("."))
    }
    if (exportGP) {
      b += ':'
      b.append(gp(prior).map(_.map(_.formatted("%.4f"))).map(_.mkString(",")).getOrElse("."))
    }

    b.result()
  }

  override def toString: String = {
    if (isGP)
      toString(exportGP = true)
    else if (isPP)
      toString(exportPP = true)
    else if (isPL)
      toString(exportPL = true)
    else
      throw new UnsupportedOperationException("Not one of GP, PL, or PP")
  }

  def pAB(theta: Double = 0.5): Option[Double] = ad.map { case Array(refDepth, altDepth) =>
    val d = new BinomialDistribution(refDepth + altDepth, theta)
    val minDepth = refDepth.min(altDepth)
    val minp = d.probability(minDepth)
    val mincp = d.cumulativeProbability(minDepth)
    (2 * mincp - minp).min(1.0).max(0.0)
  }

  def fractionReadsRef(): Option[Double] = ad.flatMap { arr => divOption(arr(0), arr.sum) }

  def toJSON: JValue = JObject(
    ("gt", gt.map(JInt(_)).getOrElse(JNull)),
    ("ad", ad.map(ads => JArray(ads.map(JInt(_)).toList)).getOrElse(JNull)),
    ("dp", dp.map(JInt(_)).getOrElse(JNull)),
    ("gq", gq.map(JInt(_)).getOrElse(JNull)),
    ("px", px.map(pxs => JArray(pxs.map(JInt(_)).toList)).getOrElse(JNull)),
    ("flags", JInt(flags)))
}

object Genotype {
  def apply(gtx: Int): Genotype = new Genotype(gtx, null, -1, -1, null, 0)

  def apply(gt: Option[Int] = None,
            ad: Option[Array[Int]] = None,
            dp: Option[Int] = None,
            gq: Option[Int] = None,
            px: Option[Array[Int]] = None,
            flags: Int = 0): Genotype = {
    new Genotype(gt.getOrElse(-1), ad.map(_.toArray).orNull, dp.getOrElse(-1), gq.getOrElse(-1), px.map(_.toArray).orNull, flags)
  }

  def schema: DataType = StructType(Array(
    StructField("gt", IntegerType),
    StructField("ad", ArrayType(IntegerType)),
    StructField("dp", IntegerType),
    StructField("gq", IntegerType),
    StructField("px", ArrayType(IntegerType)),
    StructField("flags", IntegerType)))

  final val flagMultiHasGTBit = 0x1
  final val flagMultiGTRefBit = 0x2
  final val flagBiGTMask = 0x3
  final val flagHasADBit = 0x4
  final val flagHasDPBit = 0x8
  final val flagHasGQBit = 0x10
  final val flagHasPXBit = 0x20
  final val flagSimpleADBit = 0x40
  final val flagSimpleDPBit = 0x80
  final val flagSimpleGQBit = 0x100
  final val flagFakeRefBit = 0x200
  final val flagHasGPBit = 0x400
  final val flagHasPPBit = 0x800

  def flagHasGT(isBiallelic: Boolean, flags: Int) =
    if (isBiallelic)
      (flags & flagBiGTMask) != 0
    else
      (flags & flagMultiHasGTBit) != 0

  def flagStoresGT(isBiallelic: Boolean, flags: Int) =
    isBiallelic || ((flags & flagMultiGTRefBit) != 0)

  def flagGT(isBiallelic: Boolean, flags: Int) = {
    assert(flagStoresGT(isBiallelic, flags))
    if (isBiallelic)
      (flags & flagBiGTMask) - 1
    else {
      assert((flags & flagMultiGTRefBit) != 0)
      0
    }
  }

  def flagSetGT(isBiallelic: Boolean, flags: Int, gt: Int): Int = {
    if (isBiallelic) {
      assert(gt >= 0 && gt <= 2)
      flags | ((gt & flagBiGTMask) + 1)
    } else {
      if (gt == 0)
        flags | flagMultiHasGTBit | flagMultiGTRefBit
      else
        flags | flagMultiHasGTBit
    }
  }

  def flagHasAD(flags: Int): Boolean = (flags & flagHasADBit) != 0

  def flagHasDP(flags: Int): Boolean = (flags & flagHasDPBit) != 0

  def flagHasGQ(flags: Int): Boolean = (flags & flagHasGQBit) != 0

  def flagHasPX(flags: Int): Boolean = (flags & flagHasPXBit) != 0

  def flagHasGP(flags: Int): Boolean = (flags & flagHasGPBit) != 0

  def flagHasPP(flags: Int): Boolean = (flags & flagHasPPBit) != 0

  def flagSetHasAD(flags: Int): Int = flags | flagHasADBit

  def flagSetHasDP(flags: Int): Int = flags | flagHasDPBit

  def flagSetHasGQ(flags: Int): Int = flags | flagHasGQBit

  def flagSetHasPX(flags: Int): Int = flags | flagHasPXBit

  def flagSimpleAD(flags: Int): Boolean = (flags & flagSimpleADBit) != 0

  def flagSimpleDP(flags: Int): Boolean = (flags & flagSimpleDPBit) != 0

  def flagSimpleGQ(flags: Int): Boolean = (flags & flagSimpleGQBit) != 0

  def flagSetSimpleAD(flags: Int): Int = flags | flagSimpleADBit

  def flagSetSimpleDP(flags: Int): Int = flags | flagSimpleDPBit

  def flagSetSimpleGQ(flags: Int): Int = flags | flagSimpleGQBit

  def flagFakeRef(flags: Int): Boolean = (flags & flagFakeRefBit) != 0

  def flagSetFakeRef(flags: Int): Int = flags | flagFakeRefBit

  def flagUnsetFakeRef(flags: Int): Int = flags ^ flagFakeRefBit

  def flagSetHasGP(flags: Int): Int = flags | flagHasGPBit

  def flagUnsetHasGP(flags: Int): Int = if (flagHasGP(flags)) flags ^ flagHasGPBit else flags

  def flagSetHasPP(flags: Int): Int = flags | flagHasPPBit

  def flagUnsetHasPP(flags: Int): Int = if (flagHasPP(flags)) flags ^ flagHasPPBit else flags

  def gqFromPX(px: Array[Int]): Int = {
    var m = 99
    var m2 = 99
    var i = 0
    while (i < px.length) {
      if (px(i) < m) {
        m2 = m
        m = px(i)
      } else if (px(i) < m2)
        m2 = px(i)
      i += 1
    }
    assert(m == 0)
    m2
  }

  def linearToPhredScale(a: Array[Double]): Array[Int] = linearToPhredScale(a.map{d => (d * 32768).toInt})

  def linearToPhredScale(a: Array[Int]): Array[Int] = {
    val x = a.map(phredConversionTable)
    x.map{d => (d - x.min + 0.5).toInt}
  }

  def phredToLinearScale(a: Array[Int]): Array[Double] = {
    val transformedProbs = a.map{case i => math.pow(10, i / -10.0)}
    a.map{case i => math.pow(10, i / -10.0) / transformedProbs.sum}
  }

  def renormPhredScale(a: Array[Int]): Array[Int] = a.map{_ - a.min}

  def isHomRef(gt: Int): Boolean = gt == 0

  def isHet(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j != p.k
  }

  def isHomVar(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j == p.k
  }

  def isCalledNonRef(gt: Int): Boolean = gt > 0

  def isHetNonRef(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j > 0 && p.j != p.k
  }

  def isHetRef(gt: Int): Boolean = gt > 0 && {
    val p = Genotype.gtPair(gt)
    p.j == 0 && p.k > 0
  }

  def isNotCalled(gt: Int): Boolean = gt == -1

  def isCalled(gt: Int): Boolean = gt >= 0

  def gtType(gt: Int): GenotypeType =
    if (isHomRef(gt))
      GenotypeType.HomRef
    else if (isHet(gt))
      GenotypeType.Het
    else if (isHomVar(gt))
      GenotypeType.HomVar
    else {
      assert(isNotCalled(gt))
      GenotypeType.NoCall
    }

  def nNonRefAlleles(gt: Int): Option[Int] =
    if (gt >= 0)
      Some(Genotype.gtPair(gt).nNonRefAlleles)
    else
      None

  val smallGTPair = Array(GTPair(0, 0), GTPair(0, 1), GTPair(1, 1),
    GTPair(0, 2), GTPair(1, 2), GTPair(2, 2),
    GTPair(0, 3), GTPair(1, 3), GTPair(2, 3), GTPair(3, 3),
    GTPair(0, 4), GTPair(1, 4), GTPair(2, 4), GTPair(3, 4), GTPair(4, 4),
    GTPair(0, 5), GTPair(1, 5), GTPair(2, 5), GTPair(3, 5), GTPair(4, 5), GTPair(5, 5),
    GTPair(0, 6), GTPair(1, 6), GTPair(2, 6), GTPair(3, 6), GTPair(4, 6), GTPair(5, 6),
    GTPair(6, 6),
    GTPair(0, 7), GTPair(1, 7), GTPair(7, 2), GTPair(3, 7), GTPair(4, 7),
    GTPair(5, 7), GTPair(7, 6), GTPair(7, 7))

  def gtPairRecursive(i: Int): GTPair = {
    def f(j: Int, k: Int): GTPair = if (j <= k)
      GTPair(j, k)
    else
      f(j - k - 1, k + 1)

    f(i, 0)
  }

  def gtPairSqrt(i: Int): GTPair = {
    val k: Int = (Math.sqrt(8 * i.toDouble + 1) / 2 - 0.5).toInt
    assert(k * (k + 1) / 2 <= i)
    val j = i - k * (k + 1) / 2
    assert(gtIndex(j, k) == i)
    GTPair(j, k)
  }

  def gtPair(i: Int): GTPair = {
    if (i < smallGTPair.length)
      smallGTPair(i)
    else
      gtPairSqrt(i)
  }

  def gtIndex(j: Int, k: Int): Int = {
    require(j >= 0 && j <= k, s"invalid gtIndex: ($j, $k)")
    k * (k + 1) / 2 + j
  }

  def gtIndex(p: GTPair): Int = gtIndex(p.j, p.k)

  def read(nAlleles: Int, a: ByteIterator): Genotype = {
    val isBiallelic = nAlleles == 2

    val flags = a.readULEB128()
    val isGP = flagHasGP(flags)
    val isPP = flagHasPP(flags)

    require(!(isGP && isPP), "can't have both GP and PP bits set")

    val gt: Int =
      if (flagHasGT(isBiallelic, flags)) {
        if (flagStoresGT(isBiallelic, flags))
          flagGT(isBiallelic, flags)
        else
          a.readULEB128()
      } else
        -1

    val ad: Array[Int] =
      if (flagHasAD(flags)) {
        val ada = new Array[Int](nAlleles)
        if (flagSimpleAD(flags)) {
          assert(gt >= 0)
          val p = Genotype.gtPair(gt)
          ada(p.j) = a.readULEB128()
          if (p.j != p.k)
            ada(p.k) = a.readULEB128()
        } else {
          for (i <- ada.indices)
            ada(i) = a.readULEB128()
        }
        ada
      } else
        null

    val dp =
      if (flagHasDP(flags)) {
        if (flagHasAD(flags)) {
          if (flagSimpleDP(flags))
            ad.sum
          else
            ad.sum + a.readULEB128()
        } else
          a.readULEB128()
      } else
        -1 // None

    val px: Array[Int] =
      if (flagHasPX(flags)) {
        val pxa = new Array[Int](triangle(nAlleles))
        if (gt >= 0) {
          var i = 0
          while (i < gt) {
            pxa(i) = a.readULEB128()
            i += 1
          }
          i += 1
          while (i < pxa.length) {
            pxa(i) = a.readULEB128()
            i += 1
          }
        } else {
          var i = 0
          while (i < pxa.length) {
            pxa(i) = a.readULEB128()
            i += 1
          }
        }

        if (flagHasGP(flags) && gt >= 0)
          pxa(gt) = 32768 - pxa.sum //Assuming original int values summed to 32768 or 1.0 in probability*/
        pxa
      } else
        null

    val gq: Int =
      if (flagHasGQ(flags)) {
        if (flagSimpleGQ(flags))
          gqFromPX(px)
        else
          a.readULEB128()
      } else
        -1

    new Genotype(gt, ad, dp, gq, px, flags)
  }

  def genPhredScaledPXGT(nAlleles: Int): Gen[(Option[Int], Option[Array[Int]])] = {
    val nGenotypes = triangle(nAlleles)
    val m = Int.MaxValue / (nAlleles + 1)
    for (gt: Option[Int] <- Gen.option(Gen.choose(0, nGenotypes - 1));
         px <- Gen.frequency((5,Gen.option(Gen.buildableOfN[Array[Int], Int](nGenotypes,
           Gen.choose(0, m)))),(5,Gen.option(Gen.buildableOfN[Array[Int], Int](nGenotypes,
           Gen.choose(0, 100)))))) yield {
      gt.foreach { gtx =>
        px.foreach { pxa => pxa(gtx) = 0 }
      }

      px.foreach { pxa =>
        val m = pxa.min
        var i = 0
        while (i < pxa.length) {
          pxa(i) -= m
          i += 1
        }
      }
      (gt, px)
    }
  }

  def genGP(nAlleles: Int): Gen[Genotype] = {
    val nGenotypes = triangle(nAlleles)
    for (gp <- Gen.option(Gen.buildableOfN[Array[Double], Double](nGenotypes, Gen.choose(0.0, 1.0)))) yield {

      val gpInt = gp.map{ gpa => gpa.map{case d: Double => ((d / gpa.sum) * 32768.0).round.toInt}}
      val gt = gpInt.map{gpa => if (gpa.count(_ == gpa.max) != 1) -1 else gpa.indexOf(gpa.max)}

      var flags = 0
      flags = {
        if (gpInt.isDefined) {
          flags = flagSetHasPX(flags)
          flags = flagSetHasGP(flags)
          flags
        }
        else
          flags
      }

      val g = Genotype(gt = gt, px = gpInt, flags = flags)
      g.check(nAlleles)
      g
    }
  }

  def genPP(nAlleles: Int): Gen[Genotype] = {
    for ((gt, pp) <- genPhredScaledPXGT(nAlleles)) yield {

      var flags = 0
      flags = {
        if (pp.isDefined) {
          flags = flagSetHasPX(flags)
          flags = flagSetHasPP(flags)
          flags
        } else
          flags
      }

      val g = Genotype(gt = gt, px = pp, flags = flags)
      g.check(nAlleles)
      g
    }
  }

  def genPL(nAlleles: Int): Gen[Genotype] = {
    for ((gt, pl) <- genPhredScaledPXGT(nAlleles)) yield {

      var flags = 0
      flags = {
        if (pl.isDefined) {
          flags = flagSetHasPX(flags)
          flags
        } else
          flags
      }

      val g = Genotype(gt = gt, px = pl, flags = flags)
      g.check(nAlleles)
      g
    }
  }

  def gen(nAlleles: Int): Gen[Genotype] = {
    val nGenotypes = triangle(nAlleles)
    val m = Int.MaxValue / (nAlleles + 1)
    for ((gt, px) <- genPhredScaledPXGT(nAlleles);
      ad <- Gen.option(Gen.buildableOfN[Array[Int], Int](nAlleles,
        Gen.choose(0, m)));
      dp <- Gen.option(Gen.choose(0, m));
      gq <- Gen.option(Gen.choose(0, 10000))) yield {

      val g = Genotype(gt, ad, dp.map(_ + ad.map(_.sum).getOrElse(0)), gq, px)
      g.check(nAlleles)
      g
    }
  }

  def genVariantGenotype: Gen[(Variant, Genotype)] =
    for (v <- Variant.gen;
      g <- Gen.frequency((5, gen(v.nAlleles)),(5, genGP(v.nAlleles))))
      yield (v, g)

  def genAnyGenotypeType(nAlleles: Int): Gen[Genotype] = Gen.frequency(
    (5, gen(nAlleles)),
    (5, genPL(nAlleles)),
    (5, genPP(nAlleles)),
    (5, genGP(nAlleles))
  )

  def genArb: Gen[Genotype] =
    for (nAlleles <- Gen.choose(2, 10);
         g <- genAnyGenotypeType(nAlleles))
      yield g

  implicit def arbGenotype = Arbitrary(genArb)
}

class GenotypeBuilder(nAlleles: Int) {
  require(nAlleles > 0, s"tried to create genotype builder with $nAlleles ${plural(nAlleles, "allele")}")
  val isBiallelic = nAlleles == 2
  val nGenotypes = triangle(nAlleles)

  var flags: Int = 0

  private var gt: Int = 0
  private var ad: Array[Int] = _
  private var dp: Int = 0
  private var gq: Int = 0
  private var px: Array[Int] = _

  def clear() { //FIXME: Should this clear other variables?
    flags = 0
  }

  //FIXME:
  def getPX(): Array[Int] = px

  def hasGT: Boolean =
    Genotype.flagHasGT(isBiallelic, flags)

  def setGT(newGT: Int) {
    if (newGT < 0)
      fatal(s"invalid GT value `$newGT': negative value")
    if (newGT > nGenotypes)
      fatal(s"invalid GT value `$newGT': value larger than maximum number of genotypes $nGenotypes")
    if (hasGT)
      fatal(s"invalid GT, genotype already had GT")
    flags = Genotype.flagSetGT(isBiallelic, flags, newGT)
    gt = newGT
  }

  def setAD(newAD: Array[Int]) {
    if (newAD.length != nAlleles)
      fatal(s"invalid AD field `${newAD.mkString(",")}': expected $nAlleles values, but got ${newAD.length}.")
    flags = Genotype.flagSetHasAD(flags)
    ad = newAD
  }

  def setDP(newDP: Int) {
    if (newDP < 0)
      fatal(s"invalid DP field `$newDP': negative value")
    flags = Genotype.flagSetHasDP(flags)
    dp = newDP
  }

  def setGQ(newGQ: Int) {
    if (newGQ < 0)
      fatal(s"invalid GQ field `$newGQ': negative value")
    flags = Genotype.flagSetHasGQ(flags)
    gq = newGQ
  }

  def setPX(newPX: Array[Int]) {
    if (newPX.length != nGenotypes)
      fatal(s"invalid PX field `${newPX.mkString(",")}': expected $nGenotypes values, but got ${newPX.length}.")
    flags = Genotype.flagSetHasPX(flags)
    px = newPX
  }

  def setGP(newGP: Array[Int]) {
    if (newGP.length != nGenotypes)
      fatal(s"invalid PX field `${newGP.mkString(",")}': expected $nGenotypes values, but got ${newGP.length}.")
    flags = Genotype.flagSetHasPX(flags)
    flags = Genotype.flagSetHasGP(flags)
    px = newGP
  }

  def setGP(newGP: Array[Double]) {
    setGP(newGP.map{case d => (d * 32768).toInt})
  }

  def setPP(newPP: Array[Int]) {
    if (newPP.length != nGenotypes)
      fatal(s"invalid PX field `${newPP.mkString(",")}': expected $nGenotypes values, but got ${newPP.length}.")
    flags = Genotype.flagSetHasPX(flags)
    flags = Genotype.flagSetHasPP(flags)
    px = newPP
  }

  def setPL(newPL: Array[Int]) {
    if (newPL.length != nGenotypes)
      fatal(s"invalid PX field `${newPL.mkString(",")}': expected $nGenotypes values, but got ${newPL.length}.")
    flags = Genotype.flagSetHasPX(flags)
    px = newPL
  }

  def setFakeRef() {
    flags = Genotype.flagSetFakeRef(flags)
  }

  def set(g: Genotype) {
    g.gt.foreach(setGT)
    g.ad.foreach(setAD)
    g.dp.foreach(setDP)
    g.gq.foreach(setGQ)

    if (g.isPL)
      g.px.foreach(setPL)
    else if (g.isPP)
      g.px.foreach(setPP)
    else if (g.isGP)
      g.px.foreach(setGP)
    else
      throw new UnsupportedOperationException

    if (g.fakeRef)
      setFakeRef()
  }

  def write(b: mutable.ArrayBuilder[Byte]) {
    val hasGT = Genotype.flagHasGT(isBiallelic, flags)
    val hasAD = Genotype.flagHasAD(flags)
    val hasDP = Genotype.flagHasDP(flags)
    val hasGQ = Genotype.flagHasGQ(flags)
    val hasPX = Genotype.flagHasPX(flags)
    val hasGP = Genotype.flagHasGP(flags)
    val hasPP = Genotype.flagHasPP(flags)

    var j = 0
    var k = 0
    if (hasGT) {
      val p = Genotype.gtPair(gt)
      j = p.j
      k = p.k
      if (hasAD) {
        var i = 0
        var simple = true
        while (i < ad.length && simple) {
          if (i != j && i != k && ad(i) != 0)
            simple = false
          i += 1
        }
        if (simple)
          flags = Genotype.flagSetSimpleAD(flags)
      }
    }

    var adsum = 0
    if (hasAD && hasDP) {
      adsum = ad.sum
      if (adsum == dp)
        flags = Genotype.flagSetSimpleDP(flags)
    }

    if (hasPX && hasGQ) {
      val gqFromPL = Genotype.gqFromPX(px)
      if (gq == gqFromPL)
        flags = Genotype.flagSetSimpleGQ(flags)
    }

    if (hasGP)
      flags = Genotype.flagSetHasGP(flags)
    else if (hasPP)
      flags = Genotype.flagSetHasPP(flags)

    /*
    println("flags:")
    if (Genotype.flagHasGT(isBiallelic, flags))
      println(s"  gt = $gt")
    if (Genotype.flagHasDP(flags))
      println(s"  dp = $dp")
    */

    b.writeULEB128(flags)

    if (hasGT && !Genotype.flagStoresGT(isBiallelic, flags))
      b.writeULEB128(gt)

    if (hasAD) {
      if (Genotype.flagSimpleAD(flags)) {
        b.writeULEB128(ad(j))
        if (k != j)
          b.writeULEB128(ad(k))
      } else
        ad.foreach(b.writeULEB128)
    }

    if (hasDP) {
      if (hasAD) {
        if (!Genotype.flagSimpleDP(flags))
          b.writeULEB128(dp - adsum)
      } else
        b.writeULEB128(dp)
    }

    if (hasPX) {
      if (hasGT) {
        var i = 0
        while (i < gt) {
          b.writeULEB128(px(i))
          i += 1
        }
        i += 1
        while (i < px.length) {
          b.writeULEB128(px(i))
          i += 1
        }
      } else
        px.foreach(b.writeULEB128)
    }

    if (hasGQ) {
      if (!Genotype.flagSimpleGQ(flags))
        b.writeULEB128(gq)
    }
  }
}
