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

  //def !==(that: FilterOption[T]): FilterOption[Boolean] = new FilterOption((this === that).ot.map(!_))
}

class FilterOptionBoolean(val ob: Option[Boolean]) extends AnyVal {
  def &&(that: FilterOptionBoolean): FilterOption[Boolean] = FilterOption[Boolean, Boolean, Boolean](ob, that.ob, _ && _)

  def ||(that: FilterOptionBoolean): FilterOption[Boolean] = FilterOption[Boolean, Boolean, Boolean](ob, that.ob, _ || _)

  def unary_!(): FilterOption[Boolean] = new FilterOption(ob.map(!_))
}

class FilterOptionString(val os: Option[String]) extends AnyVal {
  def toInt: FilterOption[Int] = new FilterOption(os.map(_.toInt))

  def toDouble: FilterOption[Double] = new FilterOption(os.map(_.toDouble))

  def +(that: FilterOptionString) = FilterOption[String, String, String](os, that.os, _ + _)

  def apply(i: Int): FilterOption[Char] = new FilterOption(os.map(_ (i)))
}

class FilterOptionArray[T](val oa: Option[Array[T]]) extends AnyVal {
  def apply(i: Int): FilterOption[T] = new FilterOption(oa.map(_ (i)))

  def size: FilterOption[Int] = new FilterOption(oa.map(_.length))

  def length: FilterOption[Int] = new FilterOption(oa.map(_.length))

  // FIXME
  //def ++(that: FilterOptionArray[T]): FilterOption[Array[T]] = new FilterOption(oa.flatMap(a => that.oa.map(a ++ _)))
}

class FilterOptionOrdered[T](val ot: Option[T])(implicit order: (T) => Ordered[T]) {
  def >(that: FilterOptionOrdered[T]): FilterOption[Boolean] = FilterOption[T, T, Boolean](ot, that.ot, _ > _)

  def fLt(that: FilterOptionOrdered[T]): FilterOption[Boolean] = FilterOption[T, T, Boolean](ot, that.ot, _ < _)

  def >=(that: FilterOptionOrdered[T]): FilterOption[Boolean] = FilterOption[T, T, Boolean](ot, that.ot, _ >= _)

  def fLe(that: FilterOptionOrdered[T]): FilterOption[Boolean] = FilterOption[T, T, Boolean](ot, that.ot, _ <= _)
}

class FilterOptionNumeric[T](val ot: Option[T])(implicit order: (T) => scala.math.Numeric[T]#Ops) {
  def +(that: FilterOptionNumeric[T]): FilterOption[T] = FilterOption[T, T, T](ot, that.ot, _ + _)

  def -(that: FilterOptionNumeric[T]): FilterOption[T] = FilterOption[T, T, T](ot, that.ot, _ - _)

  def *(that: FilterOptionNumeric[T]): FilterOption[T] = FilterOption[T, T, T](ot, that.ot, _ * _)

  def unary_-(): FilterOption[T] = new FilterOption(ot.map(-_))

  def abs: FilterOption[T] = new FilterOption(ot.map(_.abs()))

  def signum: FilterOption[Int] = new FilterOption(ot.map(_.signum()))

  def toInt: FilterOption[Int] = new FilterOption(ot.map(_.toInt()))

  def toLong: FilterOption[Long] = new FilterOption(ot.map(_.toLong()))

  def toFloat: FilterOption[Float] = new FilterOption(ot.map(_.toFloat()))

  def toDouble: FilterOption[Double] = new FilterOption(ot.map(_.toDouble()))
}

class FilterOptionDouble(val od: Option[Double]) {
  def *(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ * _)

  def +(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ + _)

  def -(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ - _)

  def /(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Double, Double, Double](od, that.od, _ / _)

  def fLt(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Double, Int, Boolean](od, that.oi, _ < _)

  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Double, Double, Boolean](od, that.od, _ < _)

  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(od.map(_ < that))
}

class FilterOptionLong(val ol: Option[Long]) {
  def *(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ * _)

  def *(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ * _)

  def +(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ + _)

  def +(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ + _)

  def -(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ - _)

  def -(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ - _)

  def /(that: FilterOptionLong): FilterOption[Long] = FilterOption[Long, Long, Long](ol, that.ol, _ / _)

  def /(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Long, Double, Double](ol, that.od, _ / _)

  def fLt(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Long, Long, Boolean](ol, that.ol, _ < _)

  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Long, Double, Boolean](ol, that.od, _ < _)

  def fLt(that: Long): FilterOption[Boolean] = new FilterOption(ol.map(_ < that))

  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(ol.map(_ < that))

}

class FilterOptionInt(val oi: Option[Int]) {
  def *(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ * _)

  def *(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ * _)

  def *(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ * _)

  def +(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ + _)

  def +(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ + _)

  def +(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ + _)

  def -(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ - _)

  def -(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ - _)

  def -(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ - _)

  def /(that: FilterOptionInt): FilterOption[Int] = FilterOption[Int, Int, Int](oi, that.oi, _ / _)

  def /(that: FilterOptionLong): FilterOption[Long] = FilterOption[Int, Long, Long](oi, that.ol, _ / _)

  def /(that: FilterOptionDouble): FilterOption[Double] = FilterOption[Int, Double, Double](oi, that.od, _ / _)

  def fLt(that: FilterOptionInt): FilterOption[Boolean] = FilterOption[Int, Int, Boolean](oi, that.oi, _ < _)

  def fLt(that: FilterOptionLong): FilterOption[Boolean] = FilterOption[Int, Long, Boolean](oi, that.ol, _ < _)

  def fLt(that: FilterOptionDouble): FilterOption[Boolean] = FilterOption[Int, Double, Boolean](oi, that.od, _ < _)

  def fLt(that: Int): FilterOption[Boolean] = new FilterOption(oi.map(_ < that))

  def fLt(that: Long): FilterOption[Boolean] = new FilterOption(oi.map(_ < that))

  def fLt(that: Double): FilterOption[Boolean] = new FilterOption(oi.map(_ < that))
}

object FilterUtils {
  implicit def toFilterString(s: String): FilterString = new FilterString(s)

  implicit def toFilterOption[T](t: T): FilterOption[T] = new FilterOption(Some(t))

  implicit def toFilterOptionBoolean(fo: FilterOption[Boolean]): FilterOptionBoolean = new FilterOptionBoolean(fo.ot)

  implicit def toFilterOptionString(fo: FilterOption[String]): FilterOptionString = new FilterOptionString(fo.ot)

  implicit def toFilterOptionArray[T](fo: FilterOption[Array[T]]): FilterOptionArray[T] = new FilterOptionArray[T](fo.ot)

  implicit def toFilterOptionOrdered[T](fo: FilterOption[T])(implicit order: (T) => Ordered[T]): FilterOptionOrdered[T] = new FilterOptionOrdered[T](fo.ot)

  implicit def toFilterOptionOrdered[T](t: T)(implicit order: (T) => Ordered[T]): FilterOptionOrdered[T] = new FilterOptionOrdered[T](Some(t))

  implicit def toFilterOptionNumeric[T](fo: FilterOption[T])(implicit order: (T) => scala.math.Numeric[T]#Ops): FilterOptionNumeric[T] = new FilterOptionNumeric[T](fo.ot)

  implicit def toFilterOptionNumeric[T](t: T)(implicit order: (T) => scala.math.Numeric[T]#Ops): FilterOptionNumeric[T] = new FilterOptionNumeric[T](Some(t))

  implicit def toFilterOptionDouble(v: Double): FilterOptionDouble = new FilterOptionDouble(Some(v))
  implicit def toFilterOptionDouble(fo: FilterOption[Double]): FilterOptionDouble = new FilterOptionDouble(fo.ot)

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
