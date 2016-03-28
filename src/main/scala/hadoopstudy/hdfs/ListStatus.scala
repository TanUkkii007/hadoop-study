package hadoopstudy.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}

object ListStatus extends App {
  val uri = args(0)

  val conf = new Configuration()

  val fs = FileSystem.get(URI.create(uri), conf)

  val paths = args.map(p => new Path(p))

  val status = fs.listStatus(paths)

  val listedPaths = FileUtil.stat2Paths(status)

  for (p <- listedPaths) {
    println(p)
  }
}
