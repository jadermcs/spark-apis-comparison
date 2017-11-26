import numpy as np
from scipy import stats
import sys
import numpy as np


def main():
    csv = np.genfromtxt(sys.argv[1], delimiter=',')
    x = csv[:, 0]
    y = csv[:, 1]
    slope, intercept, r_value, p_value, std_err = \
        stats.linregress(x, y)
    mslop, mintercept, lslop, uslop = stats.mstats. \
        theilslopes(y, x, np.float(sys.argv[2]))
    SST = (y**2).sum() - len(y) * y.mean()**2
    print("b1=", slope, "\tb0=", intercept)
    print("R2=", r_value**2, "\tP=", p_value)
    print("STDERR=", std_err)
    print("SST=", SST)
    print("Intervalo:")
    # t = stats.t.ppf((1 - np.float(sys.argv[2])) / 2, len(x) - 2)
    # t = t * -1
    t = np.float(sys.argv[2])
    print("Intervalo(B1) slope", slope - t * std_err, slope + t * std_err)
    SSE = r_value**2 * SST + 1
    Se = sqrt(SSE/(len(x) - 2))
    Sb0 = Se * sqrt(1/len(x) + x.mean()**2/(x**2).sum() - len(x)*x.mean()**2)
    print("Sb0: ", Sb0)

if __name__ == "__main__":
    main()
