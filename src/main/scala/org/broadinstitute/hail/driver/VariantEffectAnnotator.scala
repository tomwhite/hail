package org.broadinstitute.hail.driver

import java.io.FileInputStream

import org.kohsuke.args4j.{Option => Args4jOption}

import scala.io.Source
import scala.util.parsing.json.JSON

/**
 * Runs Ensembl VEP in a container and adds the results into the annotation object.
 */
object VariantEffectAnnotator extends DockerizedAnnotator {

  /*
  Boilerplate
   */
  def name = "vep"
  def description = "Run VEP"
  def newOptions = new Options
  override def defaultDockerImage: String = "vep-v1"
  override def dockerImage: String = options.dockerImageOption

  /*
  Options for the command
   */
  class Options extends DockerOptions {
    @Args4jOption(required = false, name = "-vc", aliases = Array("--vep-config"), usage = "Vep configuration directory")
    var vepConfig: String = System.getProperty("user.home")

    @Args4jOption(required = false, name = "-vo", aliases = Array("--vep-options"), usage = "Modifiable portions of the Vep command line enclosed in single-quotes (''). Must not modify: -i, -o, --format, --json")
    var vepOptions: String = "--everything --no_stats --cache --offline"

  }

  // The output file that VEP writes to (and which we later read back in)
  val annotationsForPartition = s"$tmpDir/$uuid-out.json"

  // Generate a script to run in the container
  def scriptContents(): String = {
    s"""
       |#!/bin/bash
       |
       |# Setup VEP's environment (~/.vep) to point to the configuration directory passed in via options
       |ln -s $host${options.vepConfig} ~/.vep
       |
       |# Run VEP
       |export PATH=$$PATH:/opt/samtools-1.2/:/opt/ensembl-tools-release-83/scripts/variant_effect_predictor/
       |variant_effect_predictor.pl -i $host$vcfForPartition -o $host$annotationsForPartition --format vcf --json ${options.vepOptions}
        """.stripMargin.trim
  }

  override def readAnnotations(): Iterator[Map[String, String]] = {
    Source
        .fromInputStream(new FileInputStream(annotationsForPartition))
        .getLines()
        .flatMap(JSON.parseFull)
        .map(_.asInstanceOf[Map[String, Any]])
        .map(_.mapValues(_.toString)) // TODO: Remove this once AnnotationData supports hierarchies
  }

}
