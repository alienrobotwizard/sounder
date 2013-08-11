#!/usr/bin/env python

import numpy as np
from matplotlib.mlab import griddata
import matplotlib.pyplot as plt


data = np.genfromtxt("data/henon-lyapunov-ab-plane.tsv", dtype=float, delimiter='\t')

a = data[:,0] # The values of the a parameter for the henon map
b = data[:,1] # The values of the b parameter for the henon map
l = data[:,2] # The values of the lyapunov exponents

lmax = max(np.absolute(l))
lmin = -lmax

# Reshape l so it lies on the appropriate rectangular grid
ai = np.unique(a)
bi = np.unique(b)
li = griddata(a,b,l,ai,bi) 

plt.pcolor(ai, bi, li, cmap='RdBu')
plt.axis('tight')
plt.xlabel("a value")
plt.ylabel("b value")
plt.colorbar()
plt.savefig("henon-lyapunov-ab-plane.pdf")
