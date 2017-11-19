import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Random
import java.io.{File, PrintWriter}

object MonteCarlo {
  def main(args: Array[String]): Unit = {
    val NUM_SAMPLES: Int = 100000000
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("MonteCarlo")
    val sc = new SparkContext(conf)

    val rnd = new Random
    val log = new PrintWriter(new File("montecarlo-scala.log" ))

    for(_ <- 1 to 10) {
      val t0: Double = System.currentTimeMillis.toDouble

      val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
        val x = rnd.nextDouble
        val y = rnd.nextDouble
        x*x + y*y < 1
      }.count()

      val t1: Double = System.currentTimeMillis.toDouble
      log.write(
        s"Elapsed time: ${(t1-t0) / 1000}\n")
      log.write(s"Pi is roughly: ${4.0 * count / NUM_SAMPLES}\n")
    }
    log.close()
  }
}
