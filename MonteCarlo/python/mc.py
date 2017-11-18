from __future__ import print_function

import random
import time

from pyspark import SparkContext


if __name__ == "__main__":
    NUM_SAMPLES = 100000000
    sc = SparkContext()
    with open("montecarlo-py.log","w") as log:
        for _ in range(10):

            def inside(p):
                x, y = random.random(), random.random()
                return x*x + y*y < 1

            t0 = time.time()

            count = sc.parallelize(range(0, NUM_SAMPLES)) \
                .filter(inside).count()
            print("Elapsed time: %f" % (time.time()-t0), file=log)
            print("Pi is roughly: %f" % (4.0*count / NUM_SAMPLES), \
                    file=log)
    sc.stop()
