package org.broadinstitute.hail.driver

import java.io.FileInputStream

import org.kohsuke.args4j.{Option => Args4jOption}

import scala.io.Source
import scala.util.parsing.json.JSON

object VariantEffectAnnotator extends DockerizedAnnotator{

  def name = "vep"
  def description = "Run VEP"

  class Options extends BaseOptions {
    @Args4jOption(required = true, name = "-vc", aliases = Array("--vep-config"), usage = "Vep configuration directory")
    var vepConfig: String = _

  }
  def newOptions = new Options
  
  override def dockerImage = "vep-v1"

  val jsonOut = s"$tmpDir/$uuid-out.json"

  // Generate a script to run in the indicated
  def scriptContents(): String = {
    s"""
       |#!/bin/bash
       |ln -s $host${options.vepConfig} ~/.vep
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
