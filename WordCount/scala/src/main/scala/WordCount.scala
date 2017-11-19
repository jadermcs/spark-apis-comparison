import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.{File, PrintWriter}
import sys.process._

object WordCount {
  def main(args: Array[String]): Unit = {
    val filein = args(0)
    val fileout = args(1)
    // create a spark context with desired configs and
    // pass to the sqlcontext
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("WCScalaApp")
    val sc = new SparkContext(conf)

    val log = new PrintWriter(new File("wordcount-scala.log" ))

    for (_ <- 1 to 10) {
      val t0: Double = System.currentTimeMillis.toDouble

      val textFile = sc.textFile("file:///" + filein)
      val counts = textFile.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
                     .sortBy(-_._2)
      counts.saveAsTextFile("file:///" + fileout)
      counts.unpersist()

      val t1: Double = System.currentTimeMillis.toDouble
      log.write(s"Elapsed time: ${(t1-t0) / 1000}\n")
      val take = s"rm -r ${fileout}".!
    }
    log.close()
    sc.stop()
  }
}
