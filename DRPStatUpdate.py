#!/usr/bin/env python

#NOTES:  allow for clustering, and allow for appending to the DRP ticket description

import sys

def drpstatupdate(pissue,drpi):
  import subprocess
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
  
  ts="0"
  #get summary from DRP ticket
  secrets = netrc.netrc()
  username,account,password = secrets.authenticators('lsstjira')  
  authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))
  issue=authenticated_jira.issue(drpi)
  summary=  issue.fields.summary
  print("summary is",summary)
  olddesc=issue.fields.description
  print("old desc is",olddesc)
  substr="{code}"
  idx = olddesc.find(substr, olddesc.find(substr) + 1)
  print(idx)
  newdesc=olddesc[0:idx]+"{code}\n"
  print("new is",newdesc)
                                     
  pattern0="(.*)#(.*)(20[0-9][0-9][0-9][0-9][0-9][0-9][Tt][0-9][0-9][0-9][0-9][0-9][0-9][Zz])"

  mts=re.match(pattern0,summary)
  if(mts):
   what=mts.group(1)
   ts=mts.group(3)
  else:
   what="0"
   ts="0"
    
  print(ts,what)
  #run butler and/or panda stats for one timestamp.
  inname="input"+str(pissue)+str(drpi)+"up.yaml"
  fup=open(inname,"w")
  fup.write("Butler: s3://butler-us-central1-panda-dev/dc2/butler-external.yaml\n")
  fup.write("Jira: "+str(pissue)+"\n")
  fup.write("collType: "+ts.upper()+"\n")
  fup.write("workNames:\n")
  fup.write("maxtask: 100")
  fup.close()


  result=subprocess.run(["python","GetButlerStat.py","-f",inname],capture_output=True,text=True)

  fbstat=open("/tmp/butlerStat-"+str(pissue)+".txt","r")
  butstat=fbstat.read()
  fbstat.close()
  
  #print("result",result)
  downname="input"+str(pissue)+str(drpi)+"lower.yaml"
  fdown=open(downname,"w")
  fdown.write("Butler: s3://butler-us-central1-panda-dev/dc2/butler-external.yaml\n")
  fdown.write("Jira: "+str(pissue)+"\n")
  fdown.write("collType: "+ts.lower()+"\n")
  fdown.write("workNames:\n")
  fdown.write("maxtask: 100")
  fdown.close()
  
  resultdown=subprocess.run(["python","GetPanDaStat.py","-f",downname],capture_output=True,text=True)
  #print("resultdown",resultdown)

  fpstat=open("/tmp/pandaStat-"+str(pissue)+".txt","r")
  statstr=fpstat.read()
  fpstat.close()
  fstat=open("/tmp/pandaWfStat-"+str(pissue)+".csv","r")
  line1=fstat.readline()
  line2=fstat.readline()
  a=line2.split(",")
  fstat.close()
  #print(len(a),a)
  pstat=a[1]
  pntasks=int(a[2][:-2])
  pnfiles=int(a[3][:-2])
  pnproc=int(a[4][:-2])
  pnfin=int(a[6][:-2])
  pnfail=int(a[7][:-2])
  psubfin=int(a[8][:-2])
  curstat = "Status:"+str(pstat)+" nTasks:"+str(pntasks)+" nFiles:"+str(pnfiles)+" nRemain:"+str(pnproc)+" nProc:"+" nFinish:"+str(pnfin)+" nFail:"+str(pnfail)+" nSubFinish:"+str(psubfin)+"\n"

  #sys.exit(1)

  pupn=ts
  #print('pupn:',pupn)
  year=str(pupn[0:4])
  month=str(pupn[4:6])
  #day=str(pupn[6:8])
  day=str("01")
  print("year:",year)
  print("year:",month)
  print("year:",day)
  link="https://panda-doma.cern.ch/tasks/?taskname=*"+pupn+"*&date_from="+str(day)+"-"+str(month)+"-"+str(year)+"&days=62&sortby=time-ascending"
  print("link:",link)
  linkline = "PanDA link:"+link+"\n"
  #print(butstat+statstr+curstat)

  nowut=datetime.now(timezone('GMT')).strftime("%Y-%m-%d %H:%M:%S")+"Z"
  
  
  issue.update(fields={'description': newdesc+butstat+linkline+statstr+curstat})
  
  print("issue:"+str(issue)+" Stats updated")
  
if __name__ == "__main__":
  nbpar = len(sys.argv)
  if nbpar < 2:
        print("Usage: DRPIssueUpate.py <bps_submit_yaml> [Production Issue] [DRP Issue(toredo)]")
        print("  <bps_submit_yaml>: yaml file used with bps submit <bps_submit_yaml> .  Should be sitting in the same dir that bps submit was done, so that the submit/ dir can be searched for more info")
        print("  [Production Issue]: PREOPS-938 or similar production issue for this group of bps submissions")
        print("  [DRP Issue]: leave off if you want a new issue generated, to redo, include the DRP-issue generated last time")
        sys.exit(-2)

  
  if(nbpar > 1):
   pissue=sys.argv[1]
  else:
   pissue="PREOPS0"

  if(nbpar >2):
   drpi=sys.argv[2]
  else:
   drpi="DRP0"

  drpstatupdate(pissue,drpi)

