package org.broadinstitute.hail.driver

import java.io.PrintWriter

import org.broadinstitute.hail.Utils._
import org.broadinstitute.hail.expr.TVariant
import org.broadinstitute.hail.methods._
import org.broadinstitute.hail.variant._
import org.broadinstitute.hail.annotations._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.broadinstitute.hail.expr
import org.objectweb.asm.ClassReader
import org.objectweb.asm.util.ASMifierClassVisitor

abstract class VariantFilter {
  def filter(v: Variant, va: Annotations): Boolean
}

class ByteArrayClassLoader(b: Array[Byte]) extends ClassLoader {
  override def findClass(name: String): Class[_] = {
    defineClass(name, b, 0, b.length)
  }
}

class SerializableVariantFilter(b: Array[Byte]) extends Serializable {
  @transient var filter_ : VariantFilter = null

  def filter: VariantFilter = {
    if (filter_ == null) {
      printTime {
        val cl = new ByteArrayClassLoader(b)
        val c = cl.findClass("org.broadinstitute.hail.driver.VariantFilter1")
        val ctor = c.getConstructor()
        filter_ = ctor.newInstance().asInstanceOf[VariantFilter]
      }
    }

    filter_
  }
}

class VariantFilter4 extends VariantFilter {
  def filter(v: Variant, va: Annotations): Boolean = {
    v.start + 500 < v.ref.toInt
  }
}

object FilterVariants extends Command {

  class Options extends BaseOptions {
    @Args4jOption(required = false, name = "--keep", usage = "Keep variants matching condition")
    var keep: Boolean = false

    @Args4jOption(required = false, name = "--remove", usage = "Remove variants matching condition")
    var remove: Boolean = false

    @Args4jOption(required = true, name = "-c", aliases = Array("--condition"),
      usage = "Filter condition: expression or .interval_list file")
    var condition: String = _
  }

  def newOptions = new Options

  def name = "filtervariants"

  def description = "Filter variants in current dataset"

  override def supportsMultiallelic = true

  def run(state: State, options: Options): State = {
    val symTab = Map(
      "v" ->(0, TVariant),
      "va" ->(1, state.vds.metadata.variantAnnotationSignatures.toExprType))

    val b = expr.Parser.parseVariantFilter(symTab, options.condition)
    val sf = new SerializableVariantFilter(b)

    // dump
    val cr = new ClassReader(b)
    val pw = new PrintWriter(System.out)
    val cv = new ASMifierClassVisitor(pw)
    cr.accept(cv, 0)

    //
    val v = Variant("foo", 37, "A", "G")
    val va = Annotations(Map.empty)
    println(sf.filter.filter(v, va))

    val vds = state.vds

    /*
    if (!options.keep && !options.remove)
      fatal(name + ": one of `--keep' or `--remove' required")

    val cond = options.condition
    val vas = vds.metadata.variantAnnotationSignatures
    val keep = options.keep
    val p: (Variant, Annotations) => Boolean = cond match {
      case f if f.endsWith(".interval_list") =>
        val ilist = IntervalList.read(options.condition, state.hadoopConf)
        val ilistBc = state.sc.broadcast(ilist)
        (v: Variant, va: Annotations) => Filter.keepThis(ilistBc.value.contains(v.contig, v.start), keep)
      case c: String =>
        val symTab = Map(
          "v" ->(0, TVariant),
          "va" ->(1, vds.metadata.variantAnnotationSignatures.toExprType))
        val a = new Array[Any](2)
        val f: () => Any = expr.Parser.parse[Any](symTab, a, options.condition)
        (v: Variant, va: Annotations) => {
          a(0) = v
          a(1) = va.attrs
          Filter.keepThisAny(f(), keep)
        }
    }
    */

    state.copy(vds = vds.filterVariants { (v: Variant, va: Annotations) => sf.filter.filter(v, va) })
  }
}

