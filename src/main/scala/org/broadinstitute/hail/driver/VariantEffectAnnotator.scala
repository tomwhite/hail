package org.broadinstitute.hail.driver

import java.io.FileInputStream

import org.kohsuke.args4j.{Option => Args4jOption}

import scala.io.Source
import scala.util.parsing.json.JSON

object VariantEffectAnnotator extends DockerizedAnnotator {

  def name = "vep"
  def description = "Run VEP"

  class Options extends DockerOptions {
    @Args4jOption(required = false, name = "-vc", aliases = Array("--vep-config"), usage = "Vep configuration directory")
    var vepConfig: String = "~/.vep"

    @Args4jOption(required = false, name = "-vo", aliases = Array("--vep-options"), usage = "Modifiable portions of the Vep command line enclosed in single-quotes (''). Must not modify: -i, -o, --format, --json")
    var vepOptions: String = "--everything --no_stats --cache --offline"

  }
  def newOptions = new Options

  override def defaultDockerImage: String = "vep-v1"

  override def dockerImage: String = options.dockerImageOption

  val jsonOut = s"$tmpDir/$uuid-out.json"

  // Generate a script to run in the indicated
  def scriptContents(): String = {
    println("Writing script contents: '"+options.vepOptions+"'")
    s"""
       |#!/bin/bash
       |ln -s $host${options.vepConfig} ~/.vep
       |export PATH=$$PATH:/opt/samtools-1.2/:/opt/ensembl-tools-release-83/scripts/variant_effect_predictor/
       |variant_effect_predictor.pl -i $host$vcfin -o $host$jsonOut --format vcf --json ${options.vepOptions}
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
