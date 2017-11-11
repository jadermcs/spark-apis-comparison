from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext
from pyspark import SparkConf


if __name__ == "__main__":
    filein = sys.argv[0]
    fileout = sys.argv[1]

    conf = SparkConf().setMaster("local[*]") \
        .setAppName("WCPythonApp")
    sc = SparkContext(conf)

    text_file = sc.textFile("file://" + filein)
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile("file://" + fileout)


spark.stop()
