#!/usr/bin/env python

import sys

def makeprodgroups(template,band,groupsize,skipgroups,ngroups,explist):
  import subprocess
  import re
  import os
  
  f=open(explist,"r")
  tempstr=os.path.basename(template)
  patt=re.compile("(.*).yaml")
  m=patt.match(tempstr)
  if(m):
   outtemp=m.group(1)
  else:
   outtemp=tempstr
  
  done=0
  lastgroup=skipgroups+ngroups
 
  highexp=0
  lowexp=0
  groupcount=int(0)
  totcount=int(0)
  for l in f:
    line=l.strip()
    if line=="":
      break
    a=line.split()
    aband=str(a[0])
    expnum=int(a[1])
    if aband != band and band!= 'all' and band != 'f':
      continue
    groupcount += 1
    totcount += 1
    curgroup= int(totcount / groupsize)
    curcount= totcount % groupsize
    if(curcount == 1):
     lowexp=expnum
    if (curcount == 0 and curgroup > skipgroups and curgroup <= lastgroup):
      highexp=expnum
      com = "sed -e s/BAND/"+str(band)+"/g -e s/GNUM/"+str(int(curgroup))+"/g -e s/LOWEXP/"+str(lowexp)+"/g -e s/HIGHEXP/"+str(highexp)+"/g " + str(template)+" >"+str(outtemp)+"_"+str(band)+"_"+str(int(curgroup))+".yaml"
      return_val=subprocess.call(com,shell=True) 
      print(com+" "+str(return_val))
  
#lastgroup
  curgroup= int(totcount / groupsize)+1
  curcount= totcount % groupsize
  if (curcount != 0 and curgroup > skipgroups and curgroup <= lastgroup):
    highexp=expnum
    com = "sed -e s/BAND/"+str(band)+"/g -e s/GNUM/"+str(int(curgroup))+"/g -e s/LOWEXP/"+str(lowexp)+"/g -e s/HIGHEXP/"+str(highexp)+"/g " + str(template)+" >"+str(outtemp)+"_"+str(band)+"_"+str(int(curgroup))+".yaml"
    return_val=subprocess.call(com,shell=True) 
    print(com+" "+str(return_val))
  
  
if __name__ == "__main__":
  nbpar = len(sys.argv)
  if nbpar < 7:
        print("Usage: MakeProdGroups.py <bps_submit_yaml_template> <band|'all'> <groupsize(visits/group)> <skipgroups(skip first skipgroups groups)> <ngroups> <explist>")
        print("  <bps_submit_yaml_template>: Template file with place holders for start/end dataset/visit/tracts (optional .yaml suffix here will be added)")
        print(" <band|'all'> Which band to restrict to (or 'all' for no restriction, matches BAND in template if not 'all')")
        print(" <groupsize> How many visits (later tracts) per group (i.e. 500)")
        print(" <skipgroups> skip <skipgroups> groups (if others generating similar campaigns")
        print(" <ngroups> how many groups (maximum)") 
        print(" <explist> text file listing <band1> <exposure1> for all visits to use")
        sys.exit(-2)

  
  template=str(sys.argv[1])
  band=str(sys.argv[2])
  groupsize=int(sys.argv[3])
  skipgroups=int(sys.argv[4])
  ngroups=int(sys.argv[5])
  explist=str(sys.argv[6])
  
  makeprodgroups(template,band,groupsize,skipgroups,ngroups,explist)

