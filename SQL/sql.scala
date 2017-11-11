import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

def main(args: Array[String]) {
  val filein = args(0)
  val fileout = args(1)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  val df = spark.read.json(filein)
