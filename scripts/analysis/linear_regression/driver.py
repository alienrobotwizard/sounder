#!/usr/bin/env python

import os
import sys
import shutil
import random
import tempfile

from org.apache.pig.scripting import Pig 

EPS          = 10e-6      # maximum distance between consective weights for convergence

pig_script   = sys.argv[1] # pig script to run iteratively
data_dir     = sys.argv[2] # directory where intermediate weights will be written
features     = sys.argv[3] # location, inside data_dir, where the data to fit exists
num_features = sys.argv[4] # number of features

#
# Cleanup data dir
#
cmd = "rmr %s/weight-*" % data_dir    
Pig.fs(cmd)

#
# Initialize weights
#
weights = []
for _ in xrange(int(num_features)):
    weights.append(str(random.random()))

fd, path = tempfile.mkstemp()    
f = open(path, 'w')
f.write("\t".join(weights)+"\n")
f.close()
os.close(fd)

copyFromLocal = "copyFromLocal %s %s/%s" % (path, data_dir, "weight-0")
Pig.fs(copyFromLocal)


#
# Iterate until converged
#
features   = "%s/%s" % (data_dir,features)
script     = Pig.compileFromFile(pig_script)
converged  = False
prev       = 0
weight_dir = tempfile.mkdtemp()
while not converged:
    input_weights  = "%s/weight-%s" % (data_dir,prev)
    output_weights = "%s/weight-%s" % (data_dir,prev+1)
  
    bound = script.bind({'input_weights':input_weights,'output_weights':output_weights,'data':features})
    bound.runSingle()
    
    if (prev > 1):
        copyToLocalPrev = "copyToLocal %s/part-r-00000 %s/weight-%s" % (input_weights, weight_dir, prev)
        copyToLocalNext = "copyToLocal %s/part-r-00000 %s/weight-%s" % (output_weights, weight_dir, prev+1)

        Pig.fs(copyToLocalPrev)
        Pig.fs(copyToLocalNext)

        localPrev = "%s/weight-%s" % (weight_dir, prev)
        localNext = "%s/weight-%s" % (weight_dir, prev+1)
        
        x1 = open(localPrev,'r').readlines()[0]
        x2 = open(localNext,'r').readlines()[0]

        x1 = [float(x.strip()) for x in x1.split("\t")]
        x2 = [float(x.strip()) for x in x2.split("\t")]

        d = sum([(pair[0] - pair[1])**2 for pair in zip(x1,x2)])

        os.remove(localPrev)
        os.remove(localNext)
        
        converged = (d < EPS)

    prev += 1
    
#
# Cleanup
#
os.remove(path)
shutil.rmtree(weight_dir)
