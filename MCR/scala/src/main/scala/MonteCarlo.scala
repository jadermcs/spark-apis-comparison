import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Random
import scala.math.pow
import java.io.{File, PrintWriter}

object MonteCarlo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("MonteCarlo")
    val sc = new SparkContext(conf)

    val rnd = new Random
    val log = new PrintWriter(new File("montecarlo-scala.log" ))

    for(i <- 1 to 9) {
      val samplexp = pow(10, i).toInt
      val t0: Double = System.currentTimeMillis.toDouble

      val count = sc.parallelize(1 to samplexp).filter { _ =>
        val x = rnd.nextDouble
        val y = rnd.nextDouble
        x*x + y*y < 1
      }.count()

      val t1: Double = System.currentTimeMillis.toDouble
      log.write(
        s"Elapsed time: ${(t1-t0) / 1000}\t NUM_SAMPLES: ${samplexp}\n")
      log.write(s"Pi is roughly: ${4.0 * count / samplexp}\n")
    }
    log.close()
  }
}
