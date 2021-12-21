#!/usr/bin/env python

#NOTES:  allow for clustering, and allow for appending to the DRP ticket description

import sys

def drpissueupdate(bpyamlfile,pissue,drpi,ts):
  import os
  import re
  import netrc
  from jira import JIRA
  from jira.client import ResultList
  from jira.resources import Issue
  from yaml import load,dump,FullLoader
  from ParseDRP import parseDRP
  from datetime import datetime
  from pytz import timezone
  
  from Parseyaml import parseyaml
  
  (bpsstr,kwd,akwd,pupn)=parseyaml(bpsyamlfile,ts)
  print('pupn:',pupn)
  year=str(pupn[0:4])
  month=str(pupn[4:6])
  #day=str(pupn[6:8])
  day=str("01")
  print("year:",year)
  print("year:",month)
  print("year:",day)
  link="https://panda-doma.cern.ch/tasks/?taskname=*"+pupn.lower()+"*&date_from="+str(day)+"-"+str(month)+"-"+str(year)+"&days=62&sortby=time-ascending"
  print("link:",link)
  dobut=0
  dopan=0
  print(dobut,dopan)
  #print(totmaxmem,nquanta,pnquanta)
  nowut=datetime.now(timezone('GMT')).strftime("%Y-%m-%d %H:%M:%S")+"Z"
  
  
  print(bpsstr,kwd,akwd)
  
  upn=kwd['campaign']+"/"+pupn
  #upn.replace("/","_")
  #upn=d['bps_defined']['uniqProcName']
  stepname=kwd['pipelineYaml']
  p=re.compile("(.*)#(.*)")
  m=p.match(stepname)
  print("stepname "+stepname)
  if m:
   steppath=m.group(1)
   stepcut=m.group(2)
  else:
   stepcut=""
  
  print("steplist "+stepcut)
  print("steppath "+steppath)
  bpsstr += "pipelineYamlSteps: "+stepcut+"\n{code}\n"
  
  print(upn+"#"+stepcut)
  sl=parseDRP(steppath,stepcut)
  tasktable="Butler Statistics\n"+"|| Step || Task || Start || nQ || sec/Q || sum(hr) || maxGB ||"+"\n"
  for s in sl:
   if(dobut==0 or s[1] not in nquanta.keys()):
    print("skipping:",s[0])
    tasktable += "|"+s[0]+"|"+s[1]+"|"+" "+ "|" + " "+ "|"+" "+"|" + " " + "|" + " " + "|" + "\n"
   else:
    tasktable += "|"+s[0]+"|"+s[1]+"|"+str(startdate[s[1]])+"|"+str(nquanta[s[1]]) + "|" + str('{:.1f}'.format(secperstep[s[1]]))+ "|" +str('{:.1f}'.format(sumtime[s[1]]))+"|"+str('{:.2f}'.format(maxmem[s[1]])) + "| \n"
  
  if(dobut==1):
    tasktable += "Total core-hours: "+str('{:.1f}'.format(totsumsec))+" Peak Memory (GB): " +str('{:.1f}'.format(totmaxmem)) + "\n"
  tasktable += "\n"
  print(tasktable)
  
  tasktable += "PanDA PREOPS: "+str(pissue)+" link:"+link+"\n"
  if(dopan==1):
   tasktable +="Panda Statistics as of: "+nowut+"\n"+"|| Step || Task || Start || PanQ || Psec/Q || wall(hr) || Psum(hr) ||parall cores||"+"\n"
   for s in sl:
    if(dopan==0 or s[1] not in pnquanta.keys()):
     tasktable += "|"+s[0]+"|"+s[1]+"|"+" "+"|"+" "+"|"+" "+ "|" + " "+ "|" + " " + "|"  + " "+"|"+"\n"
    else:
     tasktable += "|"+s[0]+"|"+s[1]+"|"+str(pstartdate[s[1]])+"|"+str(pnquanta[s[1]]) + "|" + str('{:.1f}'.format(psecperstep[s[1]]))+ "|" +str('{:.1f}'.format(pwallhr[s[1]]))+"|"+str('{:.2f}'.format(psumtime[s[1]]))+"|"+str('{:.0f}'.format(pmaxmem[s[1]])) + "| \n"
  
  
  
   #(ptotmaxmem,ptotsumsec,pnquanta,psecperstep,wallhr,sumtime,maxmem,pupn,pstat,pntasks,pnfiles,pnremain,pnproc,pnfin,pnfail,psubfin)=parsepandatable(panstepfile)
  
  if(dopan==1):
    tasktable += "Total wall-hours: "+str('{:.1f}'.format(ptotmaxmem))+" Total core-hours: " +str('{:.1f}'.format(ptotsumsec)) + "\n"
    tasktable += "Status:"+str(pstat)+" nTasks:"+str(pntasks)+" nFiles:"+str(pnfiles)+" nRemain:"+str(pnproc)+" nProc:"+" nFinish:"+str(pnfin)+" nFail:"+str(pnfail)+" nSubFinish:"+str(psubfin)+"\n"
  tasktable += "\n"
  print(tasktable)
  
  
   #(totmaxmem,totsumsec,nquanta,secperstep,sumtime,maxmem)=parsebutlertable(butstepfile)
  
  
  secrets = netrc.netrc()
  username,account,password = secrets.authenticators('lsstjira')  
  authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))
  if(drpi=="DRP0"):
   issue=authenticated_jira.create_issue(project='DRP', issuetype='Task',summary="a new issue",description=bpsstr+tasktable,components=[{"name" : "Test"}])
  else:
   issue=authenticated_jira.issue(drpi)
  
  issue.update(fields={'summary': stepcut+"#"+upn, 'description': bpsstr+tasktable})
  
  print("issue:"+str(issue))
  
  
if __name__ == "__main__":
  nbpar = len(sys.argv)
  if nbpar < 2:
        print("Usage: DRPIssueUpate.py <bps_submit_yaml> [Production Issue] [DRP Issue(toredo)]")
        print("  <bps_submit_yaml>: yaml file used with bps submit <bps_submit_yaml> .  Should be sitting in the same dir that bps submit was done, so that the submit/ dir can be searched for more info")
        print("  [Production Issue]: PREOPS-938 or similar production issue for this group of bps submissions")
        print("  [DRP Issue]: leave off if you want a new issue generated, to redo, include the DRP-issue generated last time")
        sys.exit(-2)

  
  bpsyamlfile=sys.argv[1]
  if(nbpar > 2):
   pissue=sys.argv[2]
  else:
   pissue="PREOPS0"

  if(nbpar >3):
   drpi=sys.argv[3]
  else:
   drpi="DRP0"

  if(nbpar>4):
   ts=sys.argv[4]
  else:
   ts="0"

  drpissueupdate(bpsyamlfile,pissue,drpi,ts)

