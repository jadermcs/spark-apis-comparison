from __future__ import print_function

import random
import time

from pyspark import SparkContext


if __name__ == "__main__":
    NUM_SAMPLES = 100000000
    sc = SparkContext()
    with open("gs://spark-exp/montecarlo-py.log","w") as log:
        for _ in xrange(100):
            t0 = time.time()

            def inside(p):
                x, y = random.random(), random.random()
                return x*x + y*y < 1

            count = sc.parallelize(xrange(0, NUM_SAMPLES)) \
                .filter(inside).count()
            print("Elapsed time: %f\n" % (time.time()-t0), file=log)
            print("Pi is roughly %f\n" % (4.0 * count / NUM_SAMPLES), \
                    file=log)
