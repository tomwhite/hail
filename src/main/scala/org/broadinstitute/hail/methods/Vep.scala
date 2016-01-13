package org.broadinstitute.hail.methods

import java.io._
import java.util.UUID
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.broadinstitute.hail.annotations.AnnotationData
import org.broadinstitute.hail.variant._
import scala.sys.process._
import scala.io.Source
import scala.util.parsing.json._
import scala.language.postfixOps

class DockerRunner(localTemp:String) {
  val host = "/host"
  val uuid=UUID.randomUUID().toString.replace("-", "")
  val tmpDir=new File(localTemp+"/"+uuid)
  tmpDir.mkdirs()

  def writeScript(name: String, contents: String): java.io.File = {
    val script = new java.io.File(name)
    script.createNewFile()
    script.deleteOnExit()
    val pw = new PrintWriter(script)
    pw.write(contents + "\n")
    pw.close
    script.setExecutable(true)
    script
  }

  def exec(scriptContents:String,image:String): Unit = {
    val script = writeScript(s"$tmpDir/script.sh", scriptContents)
    val cmdContent = s"docker run -iv /:$host $image $host$script"
    val cmd = writeScript(s"$tmpDir/docker.sh", cmdContent)
    cmd.getAbsolutePath !;
  }


}

object Vep {
  def name = "Vep"

  def apply(vds: VariantDataset,vepConfig:String): RDD[(Variant, AnnotationData, VepInfo, Iterable[Genotype])] = {

    val tempBase=System.getProperty("user.home")+"/hail.tmp/"

    vds.rdd.mapPartitions(iterable => {

      // This has to created _within_ the map partitions so it's executed locally on each node.
      val docker=new DockerRunner(tempBase)

      // Some file and directory names
      val container = UUID.randomUUID().toString.replace("-", "")
      val vcfin = s"${docker.tmpDir}/$container-in.vcf"
      val jsonOut = s"${docker.tmpDir}/$container-out.json"

      // Unfortunately, we need to bring in all the variants before we can start processing the vcf file.
      val data = iterable.toList

      // Generate a VCF from the variants in this partition
      val pw = new PrintWriter(vcfin)
      pw.println("##fileformat=VCFv4.1")
      pw.println("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
      for(v <- data.map(_._1))
        pw.println(v.contig+'\t'+v.start+"\t.\t"+v.ref+'\t'+v.alt+"\t.\t.\t")
      pw.close()

      // Generate a script to run in the indicated
      val scriptContents=
        s"""
           |#!/bin/bash
           |ln -s ${docker.host}$vepConfig ~/.vep
          |export PATH=$$PATH:/opt/samtools-1.2/:/opt/ensembl-tools-release-83/scripts/variant_effect_predictor/
           |variant_effect_predictor.pl --everything -i ${docker.host}$vcfin -o ${docker.host}$jsonOut --no_stats --format vcf --json --cache --offline
        """.stripMargin.trim

      docker.exec(scriptContents,"vep-v1")

      val jsonLines = Source.fromInputStream(new FileInputStream(jsonOut)).getLines()

      val vi = data.iterator
      val results=jsonLines
          .flatMap(JSON.parseFull)
          .map(_.asInstanceOf[Map[String, Any]])
          .map(j => {
        val vg=vi.next
        (vg._1, vg._2, new VepInfo(j), vg._3)
      })
          FileUtils.deleteDirectory(docker.tmpDir)

      results
    })
  }

}


case class VepInfo(map: Map[String, Any])
