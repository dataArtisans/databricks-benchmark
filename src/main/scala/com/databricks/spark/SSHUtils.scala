package com.databricks.spark

import org.apache.spark.SparkContext

import scala.sys.process._

/** Utility functions for being able to execute shell commands on executors using ssh. */
trait SSHUtils {

  val sc: SparkContext

  private def writeFile(path: String, contents: String, append: Boolean = false): Unit = {
    val fw = new java.io.FileWriter(path, append)
    fw.write(contents)
    fw.close()
  }

  lazy val publicKey = "cat /root/.ssh/id_rsa.pub".!!

  /**
   * Inject private key into executors so that the driver can ssh into them.
   */
  def addAuthorizedPublicKey(key: String): Unit = {
    writeFile("/home/ubuntu/.ssh/authorized_keys", "\n" + key, true)
  }

  /**
   * Ssh into the given `host` and execute `command`.
   */
  def ssh(host: String, command: String, logStdout: Boolean = true): String = {
    println("executing command - " + command + " on host: " + host)
    val outBuffer = new collection.mutable.ArrayBuffer[String]()
    val logger = ProcessLogger(line => outBuffer += line, println(_))

    val exitCode =
      Seq("ssh", "-o", "StrictHostKeyChecking=no", "-p", "22", "-i", "/root/.ssh/id_rsa", s"ubuntu@$host", s"$command") ! logger
    if (logStdout) {
      outBuffer.foreach(println)
    }
    if (exitCode != 0) {
      println(s"FAILED: command - $command on host: $host")
      sys.error("Command failed")
    }
    println(s"SUCCESS: command - $command on host: $host")
    outBuffer.mkString("\n")
  }

  /** Distribute the public key on executors as `authorized_keys`. */
  protected def setupSSH(numExecutors: Int): Unit = {
    val key = publicKey
    sc.parallelize(0 until numExecutors, numExecutors).foreach { i =>
      addAuthorizedPublicKey(key)
    }
  }

  /** Generate new ssh keys if required. */
  protected def generateSshKeys(): Unit = {
    if ("ls /root/.ssh/id_rsa".! > 0) {
      Seq("ssh-keygen", "-t" , "rsa", "-N", "", "-f", "/root/.ssh/id_rsa").!!
    }
  }
}
