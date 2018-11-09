package test.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object HDFSFactory {
  val conf: Configuration = new Configuration()
  conf.set("fs.defaultFS", Constants.NAME_NODE)
  var openedInstancesCount: Int = 1
  def getInstance(): FileSystem = {
    openedInstancesCount += 1
    FileSystem.newInstance(conf)
  }
}