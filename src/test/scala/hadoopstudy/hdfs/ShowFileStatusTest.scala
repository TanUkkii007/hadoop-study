package hadoopstudy.hdfs

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.scalatest.{DiagrammedAssertions, BeforeAndAfterAll, WordSpecLike}

class ShowFileStatusTest extends WordSpecLike
with BeforeAndAfterAll with DiagrammedAssertions {

  var cluster: MiniDFSCluster = _
  var fs: FileSystem = _

  "file status" must {
    "throws when accesses non-existent files" in {
      intercept[FileNotFoundException] {
        fs.getFileStatus(new Path("no-such-file"))
      }
    }

    "get information from file" in {
      val file = new Path("/dir/file")
      val stat = fs.getFileStatus(file)
      assert(stat.getPath.toUri.getPath == "/dir/file")
      assert(!stat.isDirectory)
      assert(stat.getLen == 7L)
      assert(stat.getModificationTime <= System.currentTimeMillis())
      assert(stat.getReplication == 1)
      assert(stat.getBlockSize == 128 * 1024 * 1024L)
      assert(stat.getOwner == System.getProperty("user.name"))
      assert(stat.getGroup == "supergroup")
      assert(stat.getPermission.toString == "rw-r--r--")
    }

    "get information from directory" in {
      val dir = new Path("/dir")
      val stat = fs.getFileStatus(dir)
      assert(stat.getPath.toUri.getPath == "/dir")
      assert(stat.isDirectory)
      assert(stat.getLen == 0L)
      assert(stat.getModificationTime <= System.currentTimeMillis())
      assert(stat.getReplication == 0)
      assert(stat.getBlockSize == 0L)
      assert(stat.getOwner == System.getProperty("user.name"))
      assert(stat.getGroup == "supergroup")
      assert(stat.getPermission.toString == "rwxr-xr-x")
    }

  }

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp")
    }
    cluster = new MiniDFSCluster.Builder(conf).build()
    fs = cluster.getFileSystem()
    val out = fs.create(new Path("/dir/file"))
    out.write("content".getBytes("UTF-8"))
    out.close()
  }

  override protected def afterAll(): Unit = {
    fs.close()
    cluster.shutdown()
  }
}
