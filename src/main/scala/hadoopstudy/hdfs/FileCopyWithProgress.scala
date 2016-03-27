package hadoopstudy.hdfs

import java.io.{FileInputStream, BufferedInputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path, FileSystem}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.util.Progressable

object FileCopyWithProgress extends App with LoanPattern {
  val localSrc = args(0)
  val dst = args(1)

  val in = new BufferedInputStream(new FileInputStream(localSrc))

  val conf = new Configuration()

  val fs = FileSystem.get(URI.create(dst), conf)

  val out: FSDataOutputStream = fs.create(new Path(dst), new Progressable {
    override def progress(): Unit = print(".")
  })

  IOUtils.copyBytes(in, out, 4096, true)

}
