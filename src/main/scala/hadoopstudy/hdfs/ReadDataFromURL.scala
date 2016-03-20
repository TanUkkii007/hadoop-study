package hadoopstudy.hdfs

import java.io.InputStream
import java.net.URL

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.hadoop.io.IOUtils


object ReadDataFromURL extends App {
  URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())

  val url = args(0)

  loanPattern {
    val in: InputStream = new URL(url).openStream()
    IOUtils.copyBytes(in, System.out, 4096, false)
    in
  } { in =>
    IOUtils.closeStream(in)
  }

  def loanPattern[T](fi: => T)(f: T => Unit): Unit = {
    try {
      fi
    } finally { in: T =>
      f(in)
    }
  }
}
