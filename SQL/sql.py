from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql import Row



if __name__ == "__main__":
    filein = sys.argv[0]
    fileout = sys.argv[1]

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()



    df = spark.read.json(filein)
    result = df.select("Arrival_Time").mean
    result.saveAsTextFile(fileout)

