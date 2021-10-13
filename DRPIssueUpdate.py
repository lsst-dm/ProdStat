import os
import sys
import re
import netrc
from jira import JIRA
from jira.client import ResultList
from jira.resources import Issue
from yaml import load,dump,FullLoader
import re
from ParseDRP import parseDRP

# script to read a BPS yaml submit file (after the submit to PanDA has been done, so it is
# in the submit/ subdir of the submitting user)
# and parses and reads key keyword/values and adds them to a JIRA issue in the DRP-XXXX
# JIRA ticket space.  Optionally updates an existing ticket (add DRP-XXXX) or creates a new
# one 0
# usage:  python3 drpissueupdate.py 2.2i_runs_test-med-1_w_2021_40_PREOPS-707_20211011T150425Z_config.yaml [0|DRP-5]
# 

bpsyamlfile=sys.argv[1]



kwlist=['bps_defined','campaign','computeSite','payload','requestCpus','requestMemory']

kw={'payload': ['butlerConfig','dataQuery','inCollection','sw_image'], 'bps_defined':['operator','timestamp','uniqProcName'] }

f=open(bpsyamlfile)
d=load(f,Loader=FullLoader)

bpsstr="BPS SUBMIT YAML:\n"
for k,v in d.items():
 if k in kwlist:
  if (k in kw):
     for k1 in kw[k]:
       bpsstr += str(k1)+":"+str(v[k1])+"\n"
  else:
     bpsstr += str(k)+": "+str(v)+"\n"


upn=d['bps_defined']['uniqProcName']
stepname=d['pipelineYaml']
p=re.compile(".*#(.*)")
m=p.match(stepname)
print("stepname "+stepname)
if m:
 stepcut=m.group(1)
else:
 stepcut=""

print("steplist "+stepcut)
bpsstr += "pipelineYamlSteps: "+stepcut

print(upn+"#"+stepcut)
sl=parseDRP(stepcut)
tasktable="\n"+"|| Step || Task || Status || nQuanta || Time ||"+"\n"
for s in sl:
 tasktable += "|"+s[0]+"|"+s[1]+"|"+" "+"|"+" "+ "|" +" " +"|"+ "\n"
 
tasktable += "\n"
print(tasktable)

secrets = netrc.netrc()
username,account,password = secrets.authenticators('lsstjira')  
authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))
if(sys.argv[2]=="0"):
 issue=authenticated_jira.create_issue(project='DRP', issuetype='Task',summary="a new issue",description=tasktable+bpsstr,components=[{"name" : "Test"}])
else:
 issue=authenticated_jira.issue(sys.argv[2])

issue.update(fields={'summary': stepcut+"#"+upn, 'description': tasktable+bpsstr})

print("end")


