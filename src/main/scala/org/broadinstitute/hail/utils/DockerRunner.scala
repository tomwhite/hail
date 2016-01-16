package org.broadinstitute.hail.utils

import java.io.{File, PrintWriter}
import java.util.UUID
import scala.sys.process._
import scala.language.postfixOps

/**
 * A low level trait with utilities for running scripts in docker containers.  
 */
trait DockerRunner {
  
  // TODO This shouldn't be user.home, but unfortunately that's the only directory mountable via Boot2docker + MacOS
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
