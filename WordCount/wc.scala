import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object WordCouter {
  def main(args: Array[String]): Unit = {
    val filein = args(0)
    val fileout = args(1)
    // create a spark context with desired configs and
    // pass to the sqlcontext
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("WCScalaApp")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("file://" + filein)
    val counts = textFile.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    counts.saveAsTextFile("file://" + fileout)
    sc.stop()
  }
}
