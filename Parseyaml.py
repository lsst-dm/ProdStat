#!/usr/bin/env python

import sys

def parseyaml(bpsyamlfile,ts):
  
  import os,time
  from yaml import load,dump,FullLoader
  import re
  import glob
  
  
  kwlist=['campaign','project','payload','pipelineYaml']
  kw={'payload': ['payloadName','butlerConfig','dataQuery','inCollection','sw_image','output'] }
  
  
  f=open(bpsyamlfile)
  d=load(f,Loader=FullLoader)
  f.close()
  
  kwd={}
  bpsstr="BPS Submit Keywords:\n{code}\n"
  for k,v in d.items():
   if k in kwlist:
    if (k in kw):
       for k1 in kw[k]:
         kwd[k1]=v[k1]
         bpsstr += str(k1)+":"+str(v[k1])+"\n"
    else:
       kwd[k]=v
       bpsstr += str(k)+": "+str(v)+"\n"
  
  #print(bpsstr)
  uniqid=os.path.dirname(bpsyamlfile)+"/submit/"+kwd['output']
  for k in kwd:
    v=kwd[k]
    uniqid=uniqid.replace('{'+str(k)+'}',v)
  #uniqid=uniqid.replace("/","_")

  #print(uniqid)
  
  if(ts=="0"):
    allpath=glob.glob(uniqid+'/*')
    allpath.sort()
    longpath=allpath[-1]
    ts=os.path.basename(longpath)
  else:
    ts=ts.upper()
    longpath=uniqid+"/"+ts

  #print(longpath)

  submittedyaml=kwd['output']+"_"+ts
  for k in kwd:
    v=kwd[k]
    submittedyaml=submittedyaml.replace('{'+str(k)+'}',v)
  submittedyaml=submittedyaml.replace("/","_")
  
  fullbpsyaml=longpath+"/"+submittedyaml+"_config.yaml"

  #print(fullbpsyaml)

  
  origyamlfile=longpath+"/"+os.path.basename(bpsyamlfile)

  akwd={}
  if(os.path.exists(origyamlfile)):
    (mode, ino, dev, nlink, uid, gid, size, atime, origyamlfilemtime, ctime) = os.stat(origyamlfile)
    #print(origyamlfile,origyamlfilemtime,time.ctime(origyamlfilemtime))
    
    skwlist=['bps_defined','executionButler', 'computeSite']
    skw={'bps_defined': ['operator','uniqProcName'], 'executionButler': ['queue']}
    
    f=open(fullbpsyaml)
    d=load(f,Loader=FullLoader)
    f.close()
    #print("submityaml keys:",d)
    for k,v in d.items():
     if k in skwlist:
      if (k in skw):
         for k1 in skw[k]:
           akwd[k1]=v[k1]
           bpsstr += str(k1)+":"+str(v[k1])+"\n"
      else:
         akwd[k]=v
         bpsstr += str(k)+": "+str(v)+"\n"
    
    #print("akwd",akwd)
    #print(bpsstr)
    
    
    qgraphfile=longpath+"/"+submittedyaml+".qgraph"
    (mode, ino, dev, nlink, uid, gid, qgraphfilesize, atime, mtime, ctime) = os.stat(qgraphfile)
    #print(qgraphfile,qgraphfilesize)
    bpsstr += "qgraphsize:"+str('{:.1f}'.format(qgraphfilesize/1.0e6))+"MB\n"
    qgraphout=longpath+"/"+"quantumGraphGeneration.out"
    (mode, ino, dev, nlink, uid, gid, size, atime, qgraphoutmtime, ctime) = os.stat(qgraphout)
    f=open(qgraphout)
    qgstat=f.read()
    f.close()
    m = re.search('QuantumGraph contains (.*) quanta for (.*) task',qgstat)
    if(m):
     nquanta=m.group(1)
     ntasks=m.group(2)
     bpsstr+="nTotalQuanta:"+str('{:d}'.format(int(nquanta)))+"\n" 
     bpsstr+="nTotalPanDATasks:"+str('{:d}'.format(int(ntasks)))+"\n" 
    
    #QuantumGraph contains 310365 quanta for 5 tasks
    #print(qgraphout,qgraphoutmtime,time.ctime(qgraphoutmtime))
    execbutlerdb=longpath+"/EXEC_REPO-"+submittedyaml+"/gen3.sqlite3"
    (mode, ino, dev, nlink, uid, gid, butlerdbsize, atime, butlerdbmtime, ctime) = os.stat(execbutlerdb)
    #print(execbutlerdb,butlerdbsize,butlerdbmtime,time.ctime(butlerdbmtime))
    bpsstr += "execbutlersize:"+str('{:.1f}'.format(butlerdbsize/1.0e6))+"MB"+"\n"
    timetomakeqg=qgraphoutmtime-origyamlfilemtime
    timetomakeexecbutlerdb=butlerdbmtime-qgraphoutmtime
    #print(timetomakeqg,timetomakeexecbutlerdb)
    bpsstr += "timeConstructQGraph:"+str('{:.1f}'.format(timetomakeqg/60.0))+"min\n"
    bpsstr += "timeToFillExecButlerDB:"+str('{:.1f}'.format(timetomakeexecbutlerdb/60.0))+"min\n"
    
    print(bpsstr)
  return (bpsstr,kwd,akwd,ts)

if __name__ == "__main__":
  numpar = len(sys.argv)
  print('numpar is',numpar)
  if(numpar>1):
    bpsyamlfile=sys.argv[1]

  if(numpar>2):
    ts=str(sys.argv[2])
  else:
    ts="0"

  print(bpsyamlfile,ts)
  parseyaml(bpsyamlfile,ts)
