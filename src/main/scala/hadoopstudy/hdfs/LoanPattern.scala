package hadoopstudy.hdfs

trait LoanPattern {
  def loanPattern[T](fi: => T)(f: T => Unit): Unit = {
    try {
      fi
    } finally { in: T =>
      f(in)
    }
  }
}
