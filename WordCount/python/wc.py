from __future__ import print_function

import sys
import time
import shutil
from operator import add
from pyspark import SparkContext


if __name__ == "__main__":
    filein = sys.argv[1]
    fileout = sys.argv[2]

    sc = SparkContext()

    with open("wordcount-py.log","w") as log:
        for _ in range(10):
            t0 = time.time()
            text_file = sc.textFile("file:///" + filein)
            counts = text_file.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .sortBy(lambda x: -x[1])
            counts.saveAsTextFile("file:///" + fileout)
            print("Elapsed time: %f" % (time.time()-t0), file=log)
            shutil.rmtree(fileout)

    sc.stop()
