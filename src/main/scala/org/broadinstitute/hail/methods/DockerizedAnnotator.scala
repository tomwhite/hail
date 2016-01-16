package org.broadinstitute.hail.methods

import java.io.Serializable

import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.annotations._
import org.broadinstitute.hail.driver.BaseOptions
import org.broadinstitute.hail.utils.DockerAnnotatorConfiguration
import org.broadinstitute.hail.variant._
import scala.reflect._


class DockerizedAnnotator[T <: DockerAnnotatorConfiguration[O]: ClassTag,O <: BaseOptions] extends Serializable {

  def apply(vds: VariantDataset,options:O): RDD[(Variant, AnnotationData, Iterable[Genotype])] = {
    vds.rdd.mapPartitions(stream=>{
      val d=classTag[T].runtimeClass.newInstance().asInstanceOf[T]
      d.options=options
      d.annotateAll(stream)
    })
  }


}
