package org.broadinstitute.hail.utils

import java.io.{File, PrintWriter}
import java.util.UUID

/**
 * Created by david on 1/14/16 at 11:54 PM  
 */
abstract class DockerRunner() {
  val localTemp = System.getProperty("user.home") + "/hail.tmp/"
  val host = "/host"
  val uuid = UUID.randomUUID().toString.replace("-", "")
  val tmpDir = new File(localTemp + "/" + uuid)
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

  def runDocker(): Unit = {
    val script = writeScript(s"$tmpDir/script.sh", scriptContents)
    val cmdContent = s"docker run -iv /:$host $dockerImage $host$script"
    val cmd = writeScript(s"$tmpDir/docker.sh", cmdContent)
    cmd.getAbsolutePath !;
  }

  def dockerImage: String

  def scriptContents(): String

}
