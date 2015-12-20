package org.broadinstitute.hail.methods

import org.broadinstitute.hail.Utils
import org.broadinstitute.hail.variant.{Sample, Genotype, Variant}
import scala.reflect.ClassTag
import scala.language.implicitConversions

class FilterString(val s: String) extends AnyVal {
  def ~(t: String): Boolean = s.r.findFirstIn(t).isDefined

  def !~(t: String): Boolean = !this.~(t)
}

object FilterOption {
  def apply[T](x: T): FilterOption[T] = new FilterOption[T](Some(x))

  def apply[T, S, V](a: Option[T], b: Option[S], f: (T, S) => V): FilterOption[V] =
    new FilterOption[V](a.flatMap(ax => b.map(bx => f(ax, bx))))

  def empty: FilterOption[Nothing] = new FilterOption[Nothing](None)
}

class FilterOption[+T](val ot: Option[T]) extends AnyVal {
  override def toString: String = if (ot.isDefined) ot.get.toString else "NA"

  def fEq(that: Any): FilterOption[Boolean] =
    new FilterOption(ot.flatMap(t =>
      that match {
        case s: FilterOption[_] => s.ot.map(t == _)
        case _ => Some(t == that)
      }))

  def nfEq(that: Any): FilterOption[Boolean] = new FilterOption((this fEq that).ot.map(!_))
}

class FilterOptionBoolean(val ob: Option[Boolean]) extends AnyVal {
  def fAnd(that: FilterOptionBoolean): FilterOption[Boolean] = FilterOption[Boolean, Boolean, Boolean](ob, that.ob, _ && _)

  def fOr(that: FilterOptionBoolean): FilterOption[Boolean] = FilterOption[Boolean, Boolean, Boolean](ob, that.ob, _ || _)

  def unary_!(): FilterOption[Boolean] = new FilterOption(ob.map(!_))
}

class FilterOptionString(val os: Option[String]) extends AnyVal {
  def apply(i: Int): FilterOption[Char] = new FilterOption(os.map(_ (i)))

  def length: FilterOption[Int] = new FilterOption(os.map(_.length))

  def fConcat(that: FilterOptionString) = FilterOption[String, String, String](os, that.os, _ + _)

  def toDouble: FilterOption[Double] = new FilterOption(os.map(_.toDouble))
  def toFloat: FilterOption[Float] = new FilterOption(os.map(_.toFloat))
  def toLong: FilterOption[Long] = new FilterOption(os.map(_.toLong))
  def toInt: FilterOption[Int] = new FilterOption(os.map(_.toInt))
}

class FilterOptionArray[T](val oa: Option[Array[T]]) extends AnyVal {
  def apply(i: Int): FilterOption[T] = new FilterOption(oa.map(_ (i)))

  def size: FilterOption[Int] = new FilterOption(oa.map(_.length))

  def length: FilterOption[Int] = new FilterOption(oa.map(_.length))

  //Fixme
  //def fConcat(that: FilterOptionArray[T]): FilterOption[Array[T]] = new FilterOption(oa.flatMap(a => that.oa.map(a ++ _)))
}

class FilterOptionDouble(val od: Option[Double]) {
  def fPlus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ + _)
  def fPlus(that: FilterOptionInt): FilterOption[Double] = FilterOption[Double, Int, Double](od, that.oi, _ + _)

  def fMinus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ - _)
  def fMinus(that: FilterOptionInt): FilterOption[Double] = FilterOption[Double, Int, Double](od, that.oi, _ - _)

  def fTimes(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ * _)
  def fTimes(that: FilterOptionInt): FilterOption[Double] = FilterOption[Double, Int, Double](od, that.oi, _ * _)

  def fDiv(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ / _)
  def fDiv(that: FilterOptionInt): FilterOption[Double] = FilterOption[Double, Int, Double](od, that.oi, _ / _)

  def fMod(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ % _)
  def fMod(that: FilterOptionInt): FilterOption[Double] = FilterOption[Double, Int, Double](od, that.oi, _ % _)

  def unary_-(): FilterOption[Double] = new FilterOption(od.map(- _))
  def unary_+(): FilterOption[Double] = new FilterOption(od)

  def fAbs: FilterOption[Double] = new FilterOption(od.map(_.abs))
  def fSignum: FilterOption[Int] = new FilterOption(od.map(_.signum))

  def fMax(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ max _)
  def fMax(that: Double): FilterOption[Double] = new FilterOption(od.map(_ max that))

  def fMin(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ min _)
  def fMin(that: Double): FilterOption[Double] = new FilterOption(od.map(_ min that))
  
  def fLt(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Double, Int, Boolean](od, that.oi, _ < _)
  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Double, Double, Boolean](od, that.od, _ < _)
  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(od.map(_ < that))

  def fGt(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Double, Int, Boolean](od, that.oi, _ > _)
  def fGt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Double, Double, Boolean](od, that.od, _ > _)
  def fGt(that: Double): FilterOption[Boolean] = new FilterOption(od.map(_ > that))

  def fLe(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Double, Int, Boolean](od, that.oi, _ <= _)
  def fLe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Double, Double, Boolean](od, that.od, _ <= _)
  def fLe(that: Double): FilterOption[Boolean] = new FilterOption(od.map(_ <= that))

  def fGe(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Double, Int, Boolean](od, that.oi, _ >= _)
  def fGe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Double, Double, Boolean](od, that.od, _ >= _)
  def fGe(that: Double): FilterOption[Boolean] = new FilterOption(od.map(_ >= that))
}

class FilterOptionFloat(val of: Option[Float]) {
  def fPlus(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ + _)
  def fPlus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Float, Double, Double](of, that.od, _ + _)

  def fMinus(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ - _)
  def fMinus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Float, Double, Double](of, that.od, _ - _)

  def fTimes(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ * _)
  def fTimes(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Float, Double, Double](of, that.od, _ * _)

  def fDiv(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ / _)
  def fDiv(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Float, Double, Double](of, that.od, _ / _)

  def fMod(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ % _)
  def fMod(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Float, Double, Double](of, that.od, _ % _)

  def unary_-(): FilterOption[Float] = new FilterOption(of.map(- _))
  def unary_+(): FilterOption[Float] = new FilterOption(of)

  def fAbs: FilterOption[Float] = new FilterOption(of.map(_.abs))
  def fSignum: FilterOption[Int] = new FilterOption(of.map(_.signum))

  def fMax(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ max _)
  def fMin(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Float, Float, Float](of, that.of, _ min _)

  def toDouble: FilterOption[Double] = new FilterOption(of.map(_.toDouble))

  def fLt(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Float, Float, Boolean](of, that.of, _ < _)
  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Float, Double, Boolean](of, that.od, _ < _)
  def fLt(that: Float): FilterOption[Boolean] = new FilterOption(of.map(_ < that))
  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(of.map(_ < that))

  def fGt(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Float, Float, Boolean](of, that.of, _ > _)
  def fGt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Float, Double, Boolean](of, that.od, _ > _)
  def fGt(that: Float): FilterOption[Boolean] = new FilterOption(of.map(_ > that))
  def fGt(that: Double): FilterOption[Boolean] = new FilterOption(of.map(_ > that))

  def fLe(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Float, Float, Boolean](of, that.of, _ <= _)
  def fLe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Float, Double, Boolean](of, that.od, _ <= _)
  def fLe(that: Float): FilterOption[Boolean] = new FilterOption(of.map(_ <= that))
  def fLe(that: Double): FilterOption[Boolean] = new FilterOption(of.map(_ <= that))

  def fGe(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Float, Float, Boolean](of, that.of, _ >= _)
  def fGe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Float, Double, Boolean](of, that.od, _ >= _)
  def fGe(that: Float): FilterOption[Boolean] = new FilterOption(of.map(_ >= that))
  def fGe(that: Double): FilterOption[Boolean] = new FilterOption(of.map(_ >= that))
}


class FilterOptionLong(val ol: Option[Long]) {
  def fPlus(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ + _)
  def fPlus(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Long, Float, Float](ol, that.of, _ + _)
  def fPlus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ + _)

  def fMinus(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ - _)
  def fMinus(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Long, Float, Float](ol, that.of, _ - _)
  def fMinus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ - _)

  def fTimes(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ * _)
  def fTimes(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Long, Float, Float](ol, that.of, _ * _)
  def fTimes(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ * _)

  def fDiv(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ / _)
  def fDiv(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Long, Float, Float](ol, that.of, _ / _)
  def fDiv(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ / _)

  def fMod(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ % _)
  def fMod(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Long, Float, Float](ol, that.of, _ % _)
  def fMod(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ % _)

  def unary_-(): FilterOption[Long] = new FilterOption(ol.map(- _))
  def unary_+(): FilterOption[Long] = new FilterOption(ol)
  
  def fAbs: FilterOption[Long] = new FilterOption(ol.map(_.abs))
  def fSignum: FilterOption[Int] = new FilterOption(ol.map(_.signum))

  def fMax(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ max _)
  def fMin(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ min _)

  def toDouble: FilterOption[Double] = new FilterOption(ol.map(_.toDouble))
  def toFloat: FilterOption[Float] = new FilterOption(ol.map(_.toFloat))
  
  def fLt(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Long, Long, Boolean](ol, that.ol, _ < _)
  def fLt(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Long, Float, Boolean](ol, that.of, _ < _)
  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Long, Double, Boolean](ol, that.od, _ < _)
  def fLt(that: Long): FilterOption[Boolean] = new FilterOption(ol.map(_ < that))
  def fLt(that: Float): FilterOption[Boolean] = new FilterOption(ol.map(_ < that))
  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(ol.map(_ < that))

  def fGt(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Long, Long, Boolean](ol, that.ol, _ > _)
  def fGt(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Long, Float, Boolean](ol, that.of, _ > _)
  def fGt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Long, Double, Boolean](ol, that.od, _ > _)
  def fGt(that: Long): FilterOption[Boolean] = new FilterOption(ol.map(_ > that))
  def fGt(that: Float): FilterOption[Boolean] = new FilterOption(ol.map(_ > that))
  def fGt(that: Double): FilterOption[Boolean] = new FilterOption(ol.map(_ > that))

  def fLe(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Long, Long, Boolean](ol, that.ol, _ <= _)
  def fLe(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Long, Float, Boolean](ol, that.of, _ <= _)
  def fLe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Long, Double, Boolean](ol, that.od, _ <= _)
  def fLe(that: Long): FilterOption[Boolean] = new FilterOption(ol.map(_ <= that))
  def fLe(that: Float): FilterOption[Boolean] = new FilterOption(ol.map(_ <= that))
  def fLe(that: Double): FilterOption[Boolean] = new FilterOption(ol.map(_ <= that))

  def fGe(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Long, Long, Boolean](ol, that.ol, _ >= _)
  def fGe(that: FilterOptionFloat): FilterOption[Boolean] = FilterOption[Long, Float, Boolean](ol, that.of, _ >= _)
  def fGe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Long, Double, Boolean](ol, that.od, _ >= _)
  def fGe(that: Long): FilterOption[Boolean] = new FilterOption(ol.map(_ >= that))
  def fGe(that: Float): FilterOption[Boolean] = new FilterOption(ol.map(_ >= that))
  def fGe(that: Double): FilterOption[Boolean] = new FilterOption(ol.map(_ >= that))
}

class FilterOptionInt(val oi: Option[Int]) {
  def fPlus(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ + _)
  def fPlus(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ + _)
  def fPlus(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Int, Float, Float](oi, that.of, _ + _)
  def fPlus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ + _)

  def fMinus(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ - _)
  def fMinus(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ - _)
  def fMinus(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Int, Float, Float](oi, that.of, _ - _)
  def fMinus(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ - _)

  def fTimes(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ * _)
  def fTimes(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ * _)
  def fTimes(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Int, Float, Float](oi, that.of, _ * _)
  def fTimes(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ * _)

  def fDiv(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ / _)
  def fDiv(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ / _)
  def fDiv(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Int, Float, Float](oi, that.of, _ / _)
  def fDiv(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ / _)

  def fMod(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ % _)
  def fMod(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ % _)
  def fMod(that: FilterOptionFloat): FilterOption[Float] = FilterOption[Int, Float, Float](oi, that.of, _ % _)
  def fMod(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ % _)

  def unary_-(): FilterOption[Int] = new FilterOption(oi.map(- _))
  def unary_+(): FilterOption[Int] = new FilterOption(oi)

  def fAbs: FilterOption[Int] = new FilterOption(oi.map(_.abs))
  def fSignum: FilterOption[Int] = new FilterOption(oi.map(_.signum))

  def fMax(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ max _)
  def fMin(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ min _)

  def toDouble: FilterOption[Double] = new FilterOption(oi.map(_.toDouble))
  def toFloat: FilterOption[Float] = new FilterOption(oi.map(_.toFloat))
  def toLong: FilterOption[Long] = new FilterOption(oi.map(_.toLong))

  def fLt(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Int, Int, Boolean](oi, that.oi, _ < _)
  def fLt(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Int, Long, Boolean](oi, that.ol, _ < _)
  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Int, Double, Boolean](oi, that.od, _ < _)
  def fLt(that: Int): FilterOption[Boolean] = new FilterOption(oi.map(_ < that))
  def fLt(that: Long): FilterOption[Boolean] = new FilterOption(oi.map(_ < that))
  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(oi.map(_ < that))

  def fGt(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Int, Int, Boolean](oi, that.oi, _ > _)
  def fGt(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Int, Long, Boolean](oi, that.ol, _ > _)
  def fGt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Int, Double, Boolean](oi, that.od, _ > _)
  def fGt(that: Int): FilterOption[Boolean] = new FilterOption(oi.map(_ > that))
  def fGt(that: Long): FilterOption[Boolean] = new FilterOption(oi.map(_ > that))
  def fGt(that: Double): FilterOption[Boolean] = new FilterOption(oi.map(_ > that))

  def fLe(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Int, Int, Boolean](oi, that.oi, _ <= _)
  def fLe(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Int, Long, Boolean](oi, that.ol, _ <= _)
  def fLe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Int, Double, Boolean](oi, that.od, _ <= _)
  def fLe(that: Int): FilterOption[Boolean] = new FilterOption(oi.map(_ <= that))
  def fLe(that: Long): FilterOption[Boolean] = new FilterOption(oi.map(_ <= that))
  def fLe(that: Double): FilterOption[Boolean] = new FilterOption(oi.map(_ <= that))

  def fGe(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Int, Int, Boolean](oi, that.oi, _ >= _)
  def fGe(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Int, Long, Boolean](oi, that.ol, _ >= _)
  def fGe(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Int, Double, Boolean](oi, that.od, _ >= _)
  def fGe(that: Int): FilterOption[Boolean] = new FilterOption(oi.map(_ >= that))
  def fGe(that: Long): FilterOption[Boolean] = new FilterOption(oi.map(_ >= that))
  def fGe(that: Double): FilterOption[Boolean] = new FilterOption(oi.map(_ >= that))
}

object FilterUtils {
  implicit def toFilterString(s: String): FilterString = new FilterString(s)

  implicit def toFilterOption[T](t: T): FilterOption[T] = new FilterOption(Some(t))

  implicit def toFilterOptionBoolean(b: Boolean): FilterOptionBoolean = new FilterOptionBoolean(Some(b))
  implicit def toFilterOptionBoolean(fo: FilterOption[Boolean]): FilterOptionBoolean = new FilterOptionBoolean(fo.ot)

  implicit def toFilterOptionString(s: String): FilterOptionString = new FilterOptionString(Some(s))
  implicit def toFilterOptionString(fo: FilterOption[String]): FilterOptionString = new FilterOptionString(fo.ot)

  implicit def toFilterOptionArray[T](a: Array[T]): FilterOptionArray[T] = new FilterOptionArray(Some(a))
  implicit def toFilterOptionArray[T](fo: FilterOption[Array[T]]): FilterOptionArray[T] = new FilterOptionArray[T](fo.ot)

  implicit def toFilterOptionDouble(v: Double): FilterOptionDouble = new FilterOptionDouble(Some(v))
  implicit def toFilterOptionDouble(fo: FilterOption[Double]): FilterOptionDouble = new FilterOptionDouble(fo.ot)

  implicit def toFilterOptionFloat(f: Float): FilterOptionFloat = new FilterOptionFloat(Some(f))
  implicit def toFilterOptionFloat(fo: FilterOption[Float]): FilterOptionFloat = new FilterOptionFloat(fo.ot)

  implicit def toFilterOptionLong(l: Long): FilterOptionLong = new FilterOptionLong(Some(l))
  implicit def toFilterOptionLong(fo: FilterOption[Long]): FilterOptionLong = new FilterOptionLong(fo.ot)

  implicit def toFilterOptionInt(i: Int): FilterOptionInt = new FilterOptionInt(Some(i))
  implicit def toFilterOptionInt(fo: FilterOption[Int]): FilterOptionInt = new FilterOptionInt(fo.ot)
}

object Filter {
  def keepThis(fo: FilterOption[Boolean], keep: Boolean): Boolean =
    fo.ot match {
      case Some(b) => if (keep) b else !b
      case None => false
    }
}

class Evaluator[T](t: String)(implicit tct: ClassTag[T])
  extends Serializable {
  @transient var p: Option[T] = None

  def typeCheck() {
    require(p.isEmpty)
    p = Some(Utils.eval[T](t))
  }

  def eval(): T = p match {
    case null | None =>
      val v = Utils.eval[T](t)
      p = Some(v)
      v
    case Some(v) => v
  }
}

class FilterVariantCondition(cond: String)
  extends Evaluator[(Variant) => FilterOption[Boolean]](
    "(v: org.broadinstitute.hail.variant.Variant) => { " +
      "import org.broadinstitute.hail.methods.FilterUtils._; import org.broadinstitute.hail.methods.FilterOption; " +
      cond + " }: org.broadinstitute.hail.methods.FilterOption[Boolean]") {
  def apply(v: Variant) = eval()(v)
}

class FilterSampleCondition(cond: String)
  extends Evaluator[(Sample) => FilterOption[Boolean]](
    "(s: org.broadinstitute.hail.variant.Sample) => { " +
      "import org.broadinstitute.hail.methods.FilterUtils._; import org.broadinstitute.hail.methods.FilterOption; " +
      cond + " }: org.broadinstitute.hail.methods.FilterOption[Boolean]") {
  def apply(s: Sample) = eval()(s)
}

class FilterGenotypeCondition(cond: String)
  extends Evaluator[(Variant, Sample, Genotype) => FilterOption[Boolean]](
    "(v: org.broadinstitute.hail.variant.Variant, " +
      "s: org.broadinstitute.hail.variant.Sample, " +
      "g: org.broadinstitute.hail.variant.Genotype) => { " +
      "import org.broadinstitute.hail.methods.FilterUtils._; import org.broadinstitute.hail.methods.FilterOption; " +
      cond + " }: org.broadinstitute.hail.methods.FilterOption[Boolean]") {
  def apply(v: Variant, s: Sample, g: Genotype) = eval()(v, s, g)
}
