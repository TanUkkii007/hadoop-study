package hadoopstudy.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path, FileSystem}
import org.apache.hadoop.io.IOUtils

object FileSystemCat extends App with LoanPattern {

  val uri = args(0)

  val conf = new Configuration()
  val fs = FileSystem.get(URI.create(uri), conf)

  loanPattern {
    val in: FSDataInputStream = fs.open(new Path(uri))
    IOUtils.copyBytes(in, System.out, 4096, false)
    in
  }(IOUtils.closeStream(_))

}
