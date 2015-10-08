package org.broadinstitute.k3.driver

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.broadinstitute.k3.formats.avro
import org.apache.parquet.avro.AvroParquetOutputFormat
import org.apache.parquet.hadoop.util.ContextUtil

object AvroTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("K3").setMaster("local")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration

    val chr1 = new avro.Contig("chr1", 1000)
    val chr2 = new avro.Contig("chr2", 123456)
    val variants = Array(
      new avro.Variant(chr1, 1, "A", "T"),
      new avro.Variant(chr1, 2, "ATGC", "A"),
      new avro.Variant(chr1, 4, "C", "CCCC"),
      new avro.Variant(chr1, 100, "AAA", "A")
    )
    val rdd: RDD[avro.Variant] = sc.parallelize(variants)

    val job = Job.getInstance(hadoopConf)

    println(new avro.Variant().getSchema().toString(true))

    val toSave = rdd.map(p => (null, p))
    AvroParquetOutputFormat.setSchema(job, new avro.Variant().getSchema())
    toSave.saveAsNewAPIHadoopFile("/Users/cseed/test.parquet",
      classOf[java.lang.Void],
      classOf[avro.Variant],
      classOf[AvroParquetOutputFormat[avro.Variant]],
      ContextUtil.getConfiguration(job))

    println(rdd.count())
  }
}
