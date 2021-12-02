#!/usr/bin/env python
import sys
import subprocess

template=str(sys.argv[1])
band=str(sys.argv[2])
groupsize=int(sys.argv[3])
firstgroup=int(sys.argv[4])
ngroups=int(sys.argv[5])
explist=str(sys.argv[6])

f=open(explist,"r")
done=0
lastgroup=firstgroup+ngroups
groupcount=int(0)
totcount=int(0)
for l in f:
  line=l.strip()
  if line=="":
    break
  a=line.split()
  aband=str(a[0])
  expnum=int(a[1])
  if aband != band:
    continue
  groupcount += 1
  totcount += 1
  if(totcount % groupsize == 1):
   lowexp=expnum
  curgroup= totcount / groupsize
  curcount= totcount % groupsize
  if (curcount == 0 and curgroup >= firstgroup and curgroup <= lastgroup):
    highexp=expnum
    com = "sed -e s/BAND/"+str(band)+"/g -e s/GROUPNUMBER/"+str(int(curgroup))+"/g -e s/LOWEXP/"+str(lowexp)+"/g -e s/HIGHEXP/"+str(highexp)+"/g " + str(template)+" >"+str(template)+"_"+str(band)+"_"+str(int(curgroup))+".yaml"
    return_val=subprocess.call(com,shell=True) 
    print(com+" "+str(return_val))

