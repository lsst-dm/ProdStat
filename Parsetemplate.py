def parsetemplate(bpsyamlfile):
  
  import os,time
  import sys
  from yaml import load,dump,FullLoader
  import re
  
  
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
  
  for k in kwd:
    v=kwd[k]
    bpsstr=bpsstr.replace('{'+str(k)+'}',v)

  #print(bpsstr)
  
  
  return (bpsstr,kwd)
