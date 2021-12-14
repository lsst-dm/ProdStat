#!/usr/bin/env python
import os
import sys
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
from Parsetemplate import parsetemplate

# usage:  DRPInit.py bps_group_template.yaml PREOPS-XXXX [DRP-X]
# 


def drpinit(template,pissue,drpi):
   
  (bpsstr,kwd)=parsetemplate(template)
  
  
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
  
   
  uniqid=kwd['output']
  for k in kwd:
    v=kwd[k]
    uniqid=uniqid.replace('{'+str(k)+'}',v)
  uniqid=uniqid.replace("/","_")
  #print(uniqid)
  
  secrets = netrc.netrc()
  username,account,password = secrets.authenticators('lsstjira')  
  authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))
  if(drpi=="DRP0"):
   issue=authenticated_jira.create_issue(project='DRP', issuetype='Task',summary="a new issue",description=bpsstr,components=[{"name" : "Test"}])
  else:
   issue=authenticated_jira.issue(drpi)
  
  issue.update(fields={'summary': stepcut+'#'+uniqid, 'description': bpsstr})

  jirapissue= authenticated_jira.issue(pissue)

  olddesc=jirapissue.fields.description
  
  p=re.compile("(.*)(Production Statistics:DRP-[0-9]*)",re.DOTALL)
  m=p.match(olddesc)
  if m:
   olddesc.replace(m.group(2),"Production Statistics:"+str(issue),1)
   desc=olddesc
  else:
   desc = olddesc+"\n Production Statistics:"+str(issue)+" here\n"
  
  jirapissue.update(fields={'description': desc})
  
  print("Production Issue: "+str(jirapissue)+ " description updated with DRP issue link")

  print("DRP issue for ProdStats : "+str(issue))
  
  
if __name__ == "__main__":
  nbpar = len(sys.argv)
  if nbpar < 3:
        print("Usage: DRPInit.py <bps_submit_yaml_template> <Production Issue> [DRP-issue]")
        print("  <bps_submit_yaml_template>: Template file with place holders for start/end dataset/visit/tracts (will be attached to Production Issue)")
        print("  <Production Issue>: Pre-existing issue of form PREOPS-XXX (later DRP-XXX) to update with link to ProdStat tracking issue(s) -- should match issue in template keyword ")
        print("  [DRP-issue]: If present in form DRP-XXX, redo by overwriting an existing DRP-issue. If not present, create a new DRP-issue.  All ProdStat plots and links for group of bps submits will be tracked off this DRP-issue.  Production Issue will be updated with a link to this issue, by updating description (or later by using subtask link if all are DRP type). ")
        sys.exit(-2)

  template=sys.argv[1]
  pissue=sys.argv[2]
  if(nbpar>=4):
   drpi=sys.argv[3]
  else:
   drpi="DRP0"
  
  drpinit(template,pissue,drpi)
