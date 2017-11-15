import org.apache.spark.SparkContext
import math
import java.io._

object MonteCarloPI {
  def main(args: Array[String]): Unit = {
    val NUM_SAMPLES: Int = 100000000
    val sc = new SparkContext()


    val log = new PrintWriter(new File("montecarlo-scala.log" ))

    for(_ <- 1 to 100) {
      val t0: Long = System.currentTimeMillis / 1000

      val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
        val x = math.random
        val y = math.random
        x*x + y*y < 1
      }.count()

      log.write(
        s"Elapsed time: ${System.currentTimeMillis / 1000 - t0}\n")
      log.write(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}\n")
    }
    log.close()
  }
