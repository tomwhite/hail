package org.broadinstitute.hail.driver

import java.io.{PrintWriter, Serializable}

import org.apache.commons.io.FileUtils
import org.broadinstitute.hail.utils.DockerRunner
import org.broadinstitute.hail.variant.{Genotype, Variant}
import org.broadinstitute.hail.annotations._


/**
 *   
 */
abstract class DockerizedAnnotator extends Command with DockerRunner with Serializable {
  type V = (Variant, AnnotationData, Iterable[Genotype])

  val vcfin = s"$tmpDir/$uuid-in.vcf"

  var options:Options = _

  def run(state: State, options: Options): State = {
    this.options=options
    state.vds.rdd.mapPartitions(annotateAll)
        .take(1)
        .foreach(println)

    state
  }


  // Generate a VCF from the variants in this partition
  // This implementation does not add annotations. Subclasses can override if necessary
  def writeVcf(data:List[V]): Unit = {
    val variants = data.map(_._1)
    val pw = new PrintWriter(vcfin)
    pw.println("##fileformat=VCFv4.1")
    pw.println("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT")
    for (v <- variants)
      pw.println(v.contig + '\t' + v.start + "\t.\t" + v.ref + '\t' + v.alt + "\t.\t.\t")
    pw.close()
  }

  def annotateAll(stream: Iterator[V]): Iterator[V] = {

    // Unfortunately, we need to bring in all the variants before we can start processing the vcf file.
    val data = stream.toList
    writeVcf(data)
    runDocker()

    val vi = data.iterator
    val results = readAnnotations.map(annotations => {
      val (variants,oldAnnotations,genotypes) = vi.next
      val newAnnotations = oldAnnotations.maps ++ Map(name -> annotations)
      (variants, new AnnotationData(newAnnotations, oldAnnotations.vals), genotypes)
    })
    
    FileUtils.deleteDirectory(tmpDir)

    results

  }

  def readAnnotations(): Iterator[Map[String, String]]


}
