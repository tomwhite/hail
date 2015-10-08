package org.broadinstitute.k3.variant.vsm

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.broadinstitute.k3.Utils._
import org.broadinstitute.k3.variant._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import RichRow._

object DataFrameVSM {
  def apply(sqlContext: SQLContext, metadata: VariantMetadata,
    rdd: RDD[(Variant, GenotypeStream)]): DataFrameVSM = {
    import sqlContext.implicits._

    new DataFrameVSM(metadata, rdd.toDF())
  }

  def read(sqlContext: SQLContext, dirname: String, metadata: VariantMetadata): DataFrameVSM = {
    require(dirname.endsWith(".vds"))

    val df = sqlContext.read.parquet(dirname + "/rdd.parquet")
    df.printSchema()
    new DataFrameVSM(metadata, df)
  }
}

class DataFrameVSM(val metadata: VariantMetadata,
  df: DataFrame)
  extends VariantSampleMatrix[Genotype] {
  type T = Genotype

  def nVariants: Long = df.count()
  def variants: RDD[Variant] =
    df.map(r => r.getVariant(0))

  def nPartitions: Int = df.rdd.partitions.size

  def sparkContext: SparkContext = df.rdd.sparkContext

  def cache(): VariantSampleMatrix[T] =
    new DataFrameVSM(metadata, df.cache())

  def repartition(nPartitions: Int): VariantSampleMatrix[T] =
    new DataFrameVSM(metadata, df.repartition(nPartitions))

  def count(): Long = df.count()

  def expand(): RDD[(Variant, Int, T)] =
    df.rdd.flatMap { r =>
      val v = r.getVariant(0)
      r.getGenotypeStream(1).map{ case (s, g) => (v, s, g) }
    }
  def toTupleVSM: TupleVSM[T] = new TupleVSM(metadata, expand())

  def write(sqlContext: SQLContext, dirname: String) {
    require(dirname.endsWith(".vds"))

    val hConf = sparkContext.hadoopConfiguration
    hadoopMkdir(dirname, hConf)
    writeObjectFile(dirname + "/metadata.ser", hConf)(
      _.writeObject("df" -> metadata)
    )

    df.write.parquet(dirname + "/rdd.parquet")
  }

  def mapValuesWithKeys[U](f: (Variant, Int, T) => U)
    (implicit utt: TypeTag[U], uct: ClassTag[U], iuct: ClassTag[(Int, U)]): VariantSampleMatrix[U] =
    toTupleVSM.mapValuesWithKeys(f)

  def mapWithKeys[U](f: (Variant, Int, T) => U)(implicit uct: ClassTag[U]): RDD[U] =
    toTupleVSM.mapWithKeys(f)

  def flatMapWithKeys[U](f: (Variant, Int, T) => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] =
    toTupleVSM.flatMapWithKeys(f)

  def filterVariants(p: (Variant) => Boolean): VariantSampleMatrix[T] =
    toTupleVSM.filterVariants(p)

  def filterSamples(p: (Int) => Boolean): VariantSampleMatrix[T] =
    toTupleVSM.filterSamples(p)

  def aggregateBySampleWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): Map[Int, U] =
    toTupleVSM.aggregateBySampleWithKeys(zeroValue)(seqOp, combOp)

  def aggregateByVariantWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Variant, U)] =
    toTupleVSM.aggregateByVariantWithKeys(zeroValue)(seqOp, combOp)

  def foldBySample(zeroValue: T)(combOp: (T, T) => T): Map[Int, T] =
    toTupleVSM.foldBySample(zeroValue)(combOp)

  def foldByVariant(zeroValue: T)(combOp: (T, T) => T): RDD[(Variant, T)] =
    toTupleVSM.foldByVariant(zeroValue)(combOp)
}
