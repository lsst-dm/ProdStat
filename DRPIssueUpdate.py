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

from ParseButlerTable import parsebutlertable
from ParsePanDATable import parsepandatable

# script to read a BPS yaml submit file (after the submit to PanDA has been done, so it is
# in the submit/ subdir of the submitting user)
# and parses and reads key keyword/values and adds them to a JIRA issue in the DRP-XXXX
# JIRA ticket space.  Optionally updates an existing ticket (add DRP-XXXX) or creates a new
# one 0
# usage:  python3 drpissueupdate.py 2.2i_runs_test-med-1_w_2021_40_PREOPS-707_20211011T150425Z_config.yaml [0|DRP-5]
# 

bpsyamlfile=sys.argv[1]

 
dobut=0
if(len(sys.argv) >3):
 butstepfile=sys.argv[3]
 (totmaxmem,totsumsec,nquanta,startdate,secperstep,sumtime,maxmem,upn)=parsebutlertable(butstepfile)
 dobut=1


dopan=0
if(len(sys.argv) >4):
 panstepfile=sys.argv[4]
 (ptotmaxmem,ptotsumsec,pnquanta,pstartdate,psecperstep,pwallhr,psumtime,pmaxmem,pupn,pstat,pntasks,pnfiles,pnremain,pnproc,pnfin,pnfail,psubfin)=parsepandatable(panstepfile)
 dopan=1

print('pupn:',pupn)
year=str(pupn[0:4])
month=str(pupn[4:6])
#day=str(pupn[6:8])
day=str("01")
print("year:",year)
print("year:",month)
print("year:",day)
link="https://panda-doma.cern.ch/tasks/?taskname=*"+pupn+"*&date_from="+str(day)+"-"+str(month)+"-"+str(year)+"&days=62&sortby=time-ascending"
print("link:",link)
print(dobut,dopan)
print(totmaxmem,nquanta,pnquanta)
nowut=datetime.now(timezone('GMT')).strftime("%Y-%m-%d %H:%M:%S")+"Z"

kwlist=['campaign','project','payload']

kw={'payload': ['payloadName','butlerConfig','dataQuery','inCollection','sw_image','output'] }

f=open(bpsyamlfile)
d=load(f,Loader=FullLoader)

bpsstr="BPS Submit Keywords:\n{code}\n"
for k,v in d.items():
 if k in kwlist:
  if (k in kw):
     for k1 in kw[k]:
       bpsstr += str(k1)+":"+str(v[k1])+"\n"
  else:
     bpsstr += str(k)+": "+str(v)+"\n"


upn=d['campaign']+"/"+upn
#upn.replace("/","_")
#upn=d['bps_defined']['uniqProcName']
stepname=d['pipelineYaml']
p=re.compile(".*#(.*)")
m=p.match(stepname)
print("stepname "+stepname)
if m:
 stepcut=m.group(1)
else:
 stepcut=""

print("steplist "+stepcut)
bpsstr += "pipelineYamlSteps: "+stepcut+"\n{code}\n"

print(upn+"#"+stepcut)
sl=parseDRP(stepcut)
tasktable="Butler Statistics\n"+"|| Step || Task || Start || nQ || sec/Q || sum(hr) || maxGB ||"+"\n"
for s in sl:
 if(dobut==0 or s[1] not in nquanta.keys()):
  print("skipping:",s[0])
  #tasktable += "|"+s[0]+"|"+s[1]+"|"+" "+ "|" + " "+ "|" + " " + "|" + " " + "|" + "\n"
 else:
  tasktable += "|"+s[0]+"|"+s[1]+"|"+str(startdate[s[1]])+"|"+str(nquanta[s[1]]) + "|" + str('{:.1f}'.format(secperstep[s[1]]))+ "|" +str('{:.1f}'.format(sumtime[s[1]]))+"|"+str('{:.2f}'.format(maxmem[s[1]])) + "| \n"

if(dobut==1):
  tasktable += "Total core-hours: "+str('{:.1f}'.format(totsumsec))+" Peak Memory (GB): " +str('{:.1f}'.format(totmaxmem)) + "\n"
tasktable += "\n"
print(tasktable)

tasktable += "PanDA link:"+link+"\n"
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
if(sys.argv[2]=="0"):
 issue=authenticated_jira.create_issue(project='DRP', issuetype='Task',summary="a new issue",description=bpsstr+tasktable,components=[{"name" : "Test"}])
else:
 issue=authenticated_jira.issue(sys.argv[2])

issue.update(fields={'summary': stepcut+"#"+upn, 'description': bpsstr+tasktable})

print("end")


