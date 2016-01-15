package org.broadinstitute.hail.methods

import java.io._
import java.util.UUID
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.annotations.AnnotationData
import org.broadinstitute.hail.utils.{DockerizedVariantAnnotator, DockerRunner}
import org.broadinstitute.hail.variant._
import scala.sys.process._
import scala.io.Source
import scala.util.parsing.json._
import scala.language.postfixOps






class Vep(vepConfig: String) extends Serializable {

  def apply(vds: VariantDataset): RDD[(Variant, AnnotationData, Iterable[Genotype])] = {
    vds.rdd.mapPartitions(new DockerizedVep().annotateAll)
  }

  
  def name = "Vep"

  class DockerizedVep() extends DockerizedVariantAnnotator(name) {
    override def dockerImage = "vep-v1"

    val jsonOut = s"$tmpDir/$uuid-out.json"

    // Generate a script to run in the indicated
    def scriptContents(): String = {
      s"""
         |#!/bin/bash
         |ln -s $host$vepConfig ~/.vep
         |export PATH=$$PATH:/opt/samtools-1.2/:/opt/ensembl-tools-release-83/scripts/variant_effect_predictor/
         |variant_effect_predictor.pl --everything -i $host$vcfin -o $host$jsonOut --no_stats --format vcf --json --cache --offline
        """.stripMargin.trim
    }

    override def readAnnotations(): Iterator[Map[String, String]] = {
      Source
          .fromInputStream(new FileInputStream(jsonOut))
          .getLines()
          .flatMap(JSON.parseFull)
          .map(_.asInstanceOf[Map[String, Any]])
          .map(_.mapValues(_.toString)) // TODO: Remove this once AnnotationData supports hierarchies
    }
  }


}
