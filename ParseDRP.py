def parseDRP(tocheck):

  from yaml import load,dump,FullLoader
  import os
  
  #If the DRP.yaml as put out by the Pipeline team changes -- this 
  #file should be updated.  It is in  $OBS_LSST_DIR/pipelines/imsim/DRP.yaml

  #drpfile=open('DRPtest.yaml')
  drpfile=open(os.environ.get('OBS_LSST_DIR')+"/pipelines/imsim/DRP.yaml")
  drpyaml=load(drpfile,Loader=FullLoader)
  
  #eventually need to load the includes for more details
  
  taskdict={}
  stepdict={}
  stepdesdict={}
  subsets=drpyaml['subsets']
  for k,v in subsets.items():
     stepname=k
     tasklist=v['subset']
     tasklist.insert(0,'pipetaskInit')
     tasklist.append('mergeExecutionButler')
     #print(len(tasklist))
     #print('tasklist:',tasklist)
     taskdict['pipetaskInit']=stepname
     for t in tasklist:
       taskdict[t]=stepname
     taskdict['mergeExecutionButler']=stepname
     stepdict[stepname]=tasklist
     stepdesdict[stepname]=v['description']
  
  #assumes tasknames are unique 
  #i.e. that there's not more than one step
  #with the same taskname
  #print("steps")
  #for k,v in stepdict.items():
   #print(k,v)
   #print(stepdesdict[k])
  

  steplist=tocheck.split(",")
  retdict=[]
  for i in steplist:
   if i in stepdict:
    for j in stepdict[i]:
     retdict.append([i,j])
   elif i in taskdict:
    retdict.append([taskdict[i],i])
   
  return(retdict)
