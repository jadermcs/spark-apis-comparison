import numpy as np
import sys


def main():
    csv = np.genfromtxt(sys.argv[1], delimiter=',')
    diff = csv[:,0] - csv[:,1]
    std = np.std(diff, ddof=1)
    mean = diff.mean()
    t = np.float(sys.argv[2])
    cf = t * np.sqrt(std/diff.shape[0])
    print(std)
    print(mean-cf, mean+cf)

if __name__ == "__main__":
    main()
