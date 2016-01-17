package org.broadinstitute.hail.utils

import java.io.{File, PrintWriter}
import java.util.UUID
import scala.sys.process._
import scala.language.postfixOps

/**
 * A low level trait with utilities for running scripts in docker containers.  
 */
trait DockerRunner extends Serializable{
  
  // TODO This shouldn't be user.home, but unfortunately that's the only directory mountable via Boot2docker + MacOS
  val localTemp = System.getProperty("user.home") + "/hail.tmp/"
  
  // The mount path to use within docker for passing data in/out
  val host = "/host"
  
  // Setup for temporary directories used by this run.
  val uuid = UUID.randomUUID().toString.replace("-", "")
  val tmpDir = new File(localTemp + "/" + uuid)
  tmpDir.mkdirs()

  /**
   * Utility method for writing a script for use in docker
   */
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

  /**
   * Actually run the docker
   */
  def runDocker(): Unit = {
    val script = writeScript(s"$tmpDir/script.sh", scriptContents)
    val cmdContent = s"docker run -iv /:$host $dockerImage $host$script"
    val cmd = writeScript(s"$tmpDir/docker.sh", cmdContent)
    cmd.getAbsolutePath !;
  }

  /**
   * Subclasses should override this with the image name of the docker container to load.
   * Note that the image name must be available to one of the docker repos configured on the workers. 
   */
  def dockerImage: String

  /**
   * The script to run in the docker.
   */
  def scriptContents(): String

}
