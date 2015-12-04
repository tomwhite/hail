package org.broadinstitute.hail.variant

import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.broadinstitute.hail.Utils._
import scala.language.implicitConversions

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


object VariantSampleMatrix {
  def apply(metadata: VariantMetadata,
    rdd: RDD[(Variant, Iterable[Genotype])]): VariantDataset = {
    new VariantSampleMatrix(metadata, rdd)
  }

  def read(sqlContext: SQLContext, dirname: String): VariantDataset = {
    require(dirname.endsWith(".vds"))
    import RichRow._

    val metadata = readObjectFile(dirname + "/metadata.ser", sqlContext.sparkContext.hadoopConfiguration)(
      _.readObject().asInstanceOf[VariantMetadata])

    // val df = sqlContext.read.parquet(dirname + "/rdd.parquet")
    val df = sqlContext.parquetFile(dirname + "/rdd.parquet")
    new VariantSampleMatrix[Genotype](metadata, df.rdd.map(r => (r.getVariant(0), r.getGenotypeStream(1))))
  }

  private def joinGenotypes[T,S](a:Option[Iterable[T]],b:Option[Iterable[S]],nSamples:Int)(implicit ttt: TypeTag[T], tct: ClassTag[T], stt: TypeTag[S], sct:ClassTag[S]):Iterable[(Option[T],Option[S])] = {
    require(nSamples >= 0)

    val aPrime: Iterable[Option[T]] = a match {
      case Some(x) => x.map(t => Some(t))
      case None => Array.fill[Option[T]](nSamples)(None).toIterable
    }
    val bPrime: Iterable[Option[S]] = b match {
      case Some(x) => x.map(s => Some(s))
      case None => Array.fill[Option[S]](nSamples)(None).toIterable
    }
    aPrime.zip(bPrime)
  }
}

class VariantSampleMatrix[T](val metadata: VariantMetadata,
  val localSamples: Array[Int],
  val rdd: RDD[(Variant, Iterable[T])])
  (implicit ttt: TypeTag[T], tct: ClassTag[T],
    vct: ClassTag[Variant]) {

  def this(metadata: VariantMetadata, rdd: RDD[(Variant, Iterable[T])])
    (implicit ttt: TypeTag[T], tct: ClassTag[T]) =
    this(metadata, Array.range(0, metadata.nSamples), rdd)

  def sampleIds: Array[String] = metadata.sampleIds

  def nSamples: Int = metadata.sampleIds.length

  def nLocalSamples: Int = localSamples.length

  def copy[U](metadata: VariantMetadata = this.metadata,
    localSamples: Array[Int] = this.localSamples,
    rdd: RDD[(Variant, Iterable[U])] = this.rdd)
    (implicit ttt: TypeTag[U], tct: ClassTag[U]): VariantSampleMatrix[U] =
    new VariantSampleMatrix(metadata, localSamples, rdd)

  def sparkContext: SparkContext = rdd.sparkContext

  def cache(): VariantSampleMatrix[T] = copy[T](rdd = rdd.cache())

  def repartition(nPartitions: Int) = copy[T](rdd = rdd.repartition(nPartitions))

  def nPartitions: Int = rdd.partitions.length

  def variants: RDD[Variant] = rdd.keys

  def nVariants: Long = variants.count()

  def expand(): RDD[(Variant, Int, T)] =
    mapWithKeys[(Variant, Int, T)]((v, s, g) => (v, s, g))


  def mapValues[U](f: (T) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): VariantSampleMatrix[U] = {
    mapValuesWithKeys((v, s, g) => f(g))
  }

  def mapValuesWithKeys[U](f: (Variant, Int, T) => U)
    (implicit utt: TypeTag[U], uct: ClassTag[U]): VariantSampleMatrix[U] = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    copy(rdd = rdd.map { case (v, gs) =>
      (v, localSamplesBc.value.view.zip(gs.view)
        .map { case (s, t) => f(v, s, t) })
    })
  }

  def map[U](f: T => U)(implicit uct: ClassTag[U]): RDD[U] =
    mapWithKeys((v, s, g) => f(g))

  def mapWithKeys[U](f: (Variant, Int, T) => U)(implicit uct: ClassTag[U]): RDD[U] = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    rdd
      .flatMap { case (v, gs) => localSamplesBc.value.view.zip(gs.view)
        .map { case (s, g) => f(v, s, g) }
      }
  }

  def flatMap[U](f: T => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] =
    flatMapWithKeys((v, s, g) => f(g))

  def flatMapWithKeys[U](f: (Variant, Int, T) => TraversableOnce[U])(implicit uct: ClassTag[U]): RDD[U] = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    rdd
      .flatMap { case (v, gs) => localSamplesBc.value.view.zip(gs.view)
        .flatMap { case (s, g) => f(v, s, g) }
      }
  }

  def filterVariants(ilist: IntervalList): VariantSampleMatrix[T] =
    filterVariants(v => ilist.contains(v.contig, v.start))

  def filterVariants(p: (Variant) => Boolean): VariantSampleMatrix[T] =
    copy(rdd = rdd.filter { case (v, _) => p(v) })

  def filterSamples(p: (Int) => Boolean) = {
    val localSamplesBc = sparkContext.broadcast(localSamples)
    copy[T](localSamples = localSamples.filter(p),
      rdd = rdd.map { case (v, gs) =>
        (v, localSamplesBc.value.view.zip(gs.view)
          .filter { case (s, _) => p(s) }
          .map(_._2))
      })
  }

  def aggregateBySample[U](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Int, U)] =
    aggregateBySampleWithKeys(zeroValue)((e, v, s, g) => seqOp(e, g), combOp)

  def aggregateBySampleWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Int, U)] = {

    val localSamplesBc = sparkContext.broadcast(localSamples)

    val serializer = SparkEnv.get.serializer.newInstance()
    val zeroBuffer = serializer.serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    rdd
      .mapPartitions { (it: Iterator[(Variant, Iterable[T])]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        def copyZeroValue() = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))
        val arrayZeroValue = Array.fill[U](localSamplesBc.value.length)(copyZeroValue())

        localSamplesBc.value.iterator
          .zip(it.foldLeft(arrayZeroValue) { case (acc, (v, gs)) =>
            for ((g, i) <- gs.zipWithIndex)
              acc(i) = seqOp(acc(i), v, localSamplesBc.value(i), g)
            acc
          }.iterator)
      }.foldByKey(zeroValue)(combOp)
  }

  def aggregateByVariant[U](zeroValue: U)(
    seqOp: (U, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Variant, U)] =
    aggregateByVariantWithKeys(zeroValue)((e, v, s, g) => seqOp(e, g), combOp)

  def aggregateByVariantWithKeys[U](zeroValue: U)(
    seqOp: (U, Variant, Int, T) => U,
    combOp: (U, U) => U)(implicit utt: TypeTag[U], uct: ClassTag[U]): RDD[(Variant, U)] = {

    val localSamplesBc = sparkContext.broadcast(localSamples)

    // Serialize the zero value to a byte array so that we can get a new clone of it on each key
    val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    rdd
      .map { case (v, gs) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        val zeroValue = serializer.deserialize[U](ByteBuffer.wrap(zeroArray))

        (v, gs.zipWithIndex.foldLeft(zeroValue) { case (acc, (g, i)) =>
          seqOp(acc, v, localSamplesBc.value(i), g)
        })
      }
  }

  def foldBySample(zeroValue: T)(combOp: (T, T) => T): RDD[(Int, T)] = {

    val localSamplesBc = sparkContext.broadcast(localSamples)
    val localtct = tct

    val serializer = SparkEnv.get.serializer.newInstance()
    val zeroBuffer = serializer.serialize(zeroValue)
    val zeroArray = new Array[Byte](zeroBuffer.limit)
    zeroBuffer.get(zeroArray)

    rdd
      .mapPartitions { (it: Iterator[(Variant, Iterable[T])]) =>
        val serializer = SparkEnv.get.serializer.newInstance()
        def copyZeroValue() = serializer.deserialize[T](ByteBuffer.wrap(zeroArray))(localtct)
        val arrayZeroValue = Array.fill[T](localSamplesBc.value.length)(copyZeroValue())
        localSamplesBc.value.iterator
          .zip(it.foldLeft(arrayZeroValue) { case (acc, (v, gs)) =>
            for ((g, i) <- gs.zipWithIndex)
              acc(i) = combOp(acc(i), g)
            acc
          }.iterator)
      }.foldByKey(zeroValue)(combOp)
  }

  def foldByVariant(zeroValue: T)(combOp: (T, T) => T): RDD[(Variant, T)] =
    rdd.mapValues(_.foldLeft(zeroValue)((acc, g) => combOp(acc, g)))


 /* private def mergeLocalSamples[S](other:VariantSampleMatrix[S],mergedSampleIds:Array[String])
                                  (implicit stt: TypeTag[S], sct: ClassTag[S]): Array[Int] = {
    val localIds = this.localSamples.map(this.sampleIds) ++ other.localSamples.map(other.sampleIds)
    for ((s,i) <- mergedSampleIds.zipWithIndex if localIds.contains(s)) yield i
  }

  private def reindexSamples(mergedSampleIds:Array[String],
                            mergedLocalSamples:Array[Int]):VariantSampleMatrix[Option[T]] = {

    val nLocalSamplesLocal = nLocalSamples
    val indexMapping = for (i <- localSamples) yield mergedLocalSamples.indexOf(mergedSampleIds.indexOf(sampleIds(i)))

    new VariantSampleMatrix[Option[T]](new VariantMetadata(metadata.contigLength, mergedSampleIds, metadata.vcfHeader),
      mergedLocalSamples, rdd.map { case (v, s) =>
        val newGenotypes = Array.fill[Option[T]](nLocalSamplesLocal)(None)
        for ((g, i) <- s.zipWithIndex) {
          val newIndex = indexMapping(i)
          if (newIndex != -1)
            newGenotypes(newIndex) = Some(g)
        }
        (v, newGenotypes.toIterable)
      })
  }

  def reindexSamplesInnerJoin[S](other:VariantSampleMatrix[S])
                                (implicit stt: TypeTag[S], sct: ClassTag[S]) = throw new UnsupportedOperationException

  def reindexSamplesOuterJoin[S](other:VariantSampleMatrix[S])
                                (implicit stt: TypeTag[S], sct: ClassTag[S]) = throw new UnsupportedOperationException

  def reindexSamplesLeftJoin[S](other:VariantSampleMatrix[S])
                               (implicit stt: TypeTag[S], sct: ClassTag[S]) = throw new UnsupportedOperationException

  def reindexSamplesRightJoin[S](other:VariantSampleMatrix[S])
                                (implicit stt: TypeTag[S], sct: ClassTag[S]) = {
    val mergedSampleIds = other.sampleIds
    val mergedLocalSamples = mergeLocalSamples(other,mergedSampleIds)
    val thisPrime: VariantSampleMatrix[Option[T]] = this.reindexSamples(mergedSampleIds,mergedLocalSamples)
    val otherPrime: VariantSampleMatrix[Option[S]] = other.reindexSamples(mergedSampleIds,mergedLocalSamples)
  }
*/

  def reindexSamples[S](other:VariantSampleMatrix[S],joinType:String="inner")
     (implicit stt: TypeTag[S], sct: ClassTag[S]):(VariantSampleMatrix[Option[T]],VariantSampleMatrix[Option[S]]) = {

    val mergedSampleIds: Array[String] = joinType match {
      case "inner" => this.sampleIds.toSet.intersect(other.sampleIds.toSet).toArray
      case "outer" => this.sampleIds.toSet.union(other.sampleIds.toSet).toArray
      case "left" => this.sampleIds
      case "right" => other.sampleIds
      case _ => throw new UnsupportedOperationException
    }

    val mergedLocalSamples: Array[Int] = {
      val localIds = this.localSamples.map(this.sampleIds) ++ other.localSamples.map(other.sampleIds)
      for ((s,i) <- mergedSampleIds.zipWithIndex if localIds.contains(s)) yield i
    }

    val indexMapping: Array[Int] = for (i <- localSamples) yield mergedLocalSamples.indexOf(mergedSampleIds.indexOf(sampleIds(i)))

    val thisPrime = new VariantSampleMatrix[Option[T]](new VariantMetadata(this.metadata.contigLength, mergedSampleIds, this.metadata.vcfHeader),
      mergedLocalSamples, rdd.map { case (v, s) =>
        val newGenotypes = Array.fill[Option[T]](mergedLocalSamples.length)(None)
        for ((g, i) <- s.zipWithIndex) {
          val newIndex = indexMapping(i)
          if (newIndex != -1)
            newGenotypes(newIndex) = Some(g)
        }
        (v, newGenotypes.toIterable)
      })

    val otherPrime = new VariantSampleMatrix[Option[S]](new VariantMetadata(other.metadata.contigLength, mergedSampleIds, other.metadata.vcfHeader),
      mergedLocalSamples, other.rdd.map { case (v, s) =>
        val newGenotypes = Array.fill[Option[S]](mergedLocalSamples.length)(None)
        for ((g, i) <- s.zipWithIndex) {
          val newIndex = indexMapping(i)
          if (newIndex != -1)
            newGenotypes(newIndex) = Some(g)
        }
        (v, newGenotypes.toIterable)
      })

    (thisPrime, otherPrime)
  }

  // Variant Joins

  def fullOuterJoin[S](other:VariantSampleMatrix[S])(implicit stt: TypeTag[S], sct: ClassTag[S]): VariantSampleMatrix[(Option[T],Option[S])] = {
    import VariantSampleMatrix._
    require(this.sampleIds.sameElements(other.sampleIds) && this.localSamples.sameElements(other.localSamples))
    val nSamplesLocal = this.nLocalSamples
    val tttLocal = ttt
    val tctLocal = tct
    val sttLocal = stt
    val sctLocal = sct
    new VariantSampleMatrix[(Option[T],Option[S])](new VariantMetadata(this.metadata.contigLength,this.sampleIds,this.metadata.vcfHeader),
      this.localSamples,
      this.rdd.fullOuterJoin(other.rdd)
          .map[(Variant, Iterable[(Option[T], Option[S])])]{case (v,(a,b)) => (v,joinGenotypes(a,b,nSamplesLocal)(tttLocal, tctLocal, sttLocal, sctLocal))}
    )
  }

  def leftOuterJoin[S](other:VariantSampleMatrix[S])(implicit stt: TypeTag[S], sct: ClassTag[S]): VariantSampleMatrix[(Option[T],Option[S])] = {
    import VariantSampleMatrix._
    require(this.sampleIds.sameElements(other.sampleIds) && this.localSamples.sameElements(other.localSamples))
    val nSamplesBc = sparkContext.broadcast(this.nLocalSamples)
    new VariantSampleMatrix[(Option[T],Option[S])](new VariantMetadata(this.metadata.contigLength,this.sampleIds,this.metadata.vcfHeader),
      this.localSamples,
      this.rdd.leftOuterJoin(other.rdd)
        .map{case (v,(a,b)) => (v,joinGenotypes(Some(a),b,nSamplesBc.value))}
    )
  }

  def rightOuterJoin[S](other:VariantSampleMatrix[S])(implicit stt: TypeTag[S], sct: ClassTag[S]): VariantSampleMatrix[(Option[T],Option[S])] = {
    import VariantSampleMatrix._
    require(this.sampleIds.sameElements(other.sampleIds) && this.localSamples.sameElements(other.localSamples))
    val nSamplesBc = sparkContext.broadcast(this.nLocalSamples)
    new VariantSampleMatrix[(Option[T],Option[S])](new VariantMetadata(this.metadata.contigLength,this.sampleIds,this.metadata.vcfHeader),
      this.localSamples,
      this.rdd.rightOuterJoin(other.rdd)
        .map{case (v,(a,b)) => (v,joinGenotypes(a,Some(b),nSamplesBc.value))}
    )
  }

  def innerJoin[S](other:VariantSampleMatrix[S])(implicit stt: TypeTag[S], sct: ClassTag[S]): VariantSampleMatrix[(Option[T],Option[S])] = {
    import VariantSampleMatrix._
    require(this.sampleIds.sameElements(other.sampleIds) && this.localSamples.sameElements(other.localSamples))
    val nSamplesBc = sparkContext.broadcast(this.nLocalSamples)
    new VariantSampleMatrix[(Option[T],Option[S])](new VariantMetadata(this.metadata.contigLength,this.sampleIds,this.metadata.vcfHeader),
      this.localSamples,
      this.rdd.join(other.rdd)
        .map{case (v,(a,b)) => (v,joinGenotypes(Some(a),Some(b),nSamplesBc.value))}
    )
  }
}

// FIXME AnyVal Scala 2.11
class RichVDS(vds: VariantDataset) {

  def write(sqlContext: SQLContext, dirname: String, compress: Boolean = true) {
    import sqlContext.implicits._

    require(dirname.endsWith(".vds"))

    val hConf = vds.sparkContext.hadoopConfiguration
    hadoopMkdir(dirname, hConf)
    writeObjectFile(dirname + "/metadata.ser", hConf)(
      _.writeObject(vds.metadata))

    // rdd.toDF().write.parquet(dirname + "/rdd.parquet")
    vds.rdd
      .map { case (v, gs) => (v, gs.toGenotypeStream(v, compress)) }
      .toDF()
      .saveAsParquetFile(dirname + "/rdd.parquet")
  }
}
