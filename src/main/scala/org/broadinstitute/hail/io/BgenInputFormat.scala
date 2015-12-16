package org.broadinstitute.hail.io

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred._
import org.broadinstitute.hail.variant.Variant

class BgenInputFormat extends IndexedBinaryInputFormat[Variant] {
  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable,
    ParsedLine[Variant]] = {
    reporter.setStatus(split.toString)
    new BgenBlockReader(job, split.asInstanceOf[FileSplit])
  }
}
