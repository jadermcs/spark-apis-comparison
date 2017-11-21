from __future__ import print_function

import random
import time

from pyspark import SparkContext


if __name__ == "__main__":
    NUM_SAMPLES = 1000
    sc = SparkContext()
    with open("montecarlo-py.log","w") as log:
        for i in range(10):

            def inside(p):
                x, y = random.random(), random.random()
                return x*x + y*y < 1

            t0 = time.time()

            samplexp = NUM_SAMPLES * 10**i

            count = sc.parallelize(range(0, samplexp)) \
                .filter(inside).count()
            print("Elapsed time: %f\tNUM_SAMPLES: %d" % \
                    (time.time()-t0, samplexp), file=log)
            print("Pi is roughly: %f" % (4.0*count / samplexp), \
                    file=log)
    sc.stop()
