#!/usr/bin/env python

import json
import sys
import re
import netrc
from jira import JIRA
from jira.client import ResultList
from jira.resources import Issue

def parseissuedesc(jdesc,jsummary):
 
 pattern0="(.*)#(.*)(20[0-9][0-9][0-9][0-9][0-9][0-9][Tt][0-9][0-9][0-9][0-9][0-9][0-9][Zz])"
 
 mts=re.match(pattern0,jsummary)
 if(mts):
   what=mts.group(1)
   ts=mts.group(3)
 else:
   what="0"
   ts="0"
 #print("ts:",ts)
 #print(jdesc)
 jlines=jdesc.splitlines()
 L=iter(jlines)
 pattern1=re.compile("(.*)tract in (.*)")
 pattern2=re.compile("(.*)exposure >=(.*) and exposure <=(.*)")
 pattern3=re.compile("(.*)Status:.*nTasks:(.*)nFiles:(.*)nRemain.*nProc: nFinish:(.*) nFail:(.*) nSubFinish:(.*)")
 #pattern3=re.compile("(.*)Status:(.*)")
 pattern4=re.compile("(.*)PanDA.*link:(.*)")
 hilow="()"
 status=[0,0,0,0,0]
 pandalink=""
 for l in L:
   n1=pattern1.match(l)
   if(n1):
     #print("Tract range:",n1.group(2),":end")
     hilow=n1.group(2)
     #print("hilow:",hilow)
   n2=pattern2.match(l)
   if(n2):
     print("exposurelo:",n2.group(2)," exphigh:",n2.group(3),":end")
     hilow="("+str(int(n2.group(2)))+","+str(int(n2.group(3)))+")"
     #print("hilow:",hilow)
   #else:
     #print("no match to l",l)
   n3=pattern3.match(l)
   if(n3):
     #print("match is",n3.group(1),n3.group(2))
     #print("(.*)Status: finished nTasks:(.*)nFiles:(.*)nRemain:(.*)nProc: nFinish:(.*) nFail:(.*) nSubFinish:(.*)")
     #sys.exit(1)
     statNtasks=int(n3.group(2))
     statNfiles=int(n3.group(3))
     statNFinish=int(n3.group(4))
     statNFail=int(n3.group(5))
     statNSubFin=int(n3.group(6))
     #print("Job status tasks,files,finish,fail,subfin:",statNtasks,statNfiles,statNFinish,statNFail,statNSubFin)
     status=[statNtasks,statNfiles,statNFinish,statNFail,statNSubFin]
   m=pattern4.match(l) 
   if(m):
     pandalink=m.group(2)
     #print("pandalink:",pandaline)

 #sys.exit(1)

 return(ts,status,hilow,pandalink,what)

def dicttotable(dict,sorton):

 dictheader=['Date','PREOPS','STATS','(T,Q,D,Fa,Sf)','PANDA','DESCRIP']

 table="||"
 for i in dictheader:
   table += str(i)+"||"
 table += "\n"
 
 for i in sorted(dict.keys(),reverse=True):
   pis=i.split("#")[0]
   ts=i.split("#")[1]
   status=dict[i][2]
   nT=status[0]
   nFile=status[1]
   nFin=status[2]
   nFail=status[3]
   nSubF=status[4]
   statstring=str(nT)+","+str(nFile)+","+str(nFin)+","+str(nFail)+","+str(nSubF)
   scolor="black"
   #print(statstring,nT,nFile,nFin,nFail,nSubF)
   if(nFail>0):
    scolor="red"
   if(nT==nFin+nSubF):
    scolor="black"
   if(nT==nFin):
    scolor="green"
   if(int(nFail)==0 and int(nFile)==0):
    scolor="blue"

   longdatetime=ts
   shortyear=str(longdatetime[0:4])
   shortmon=str(longdatetime[4:6])
   shortday=str(longdatetime[6:8])
   #print(shortyear,shortmon,shortday)

   what=dict[i][4]
   if(len(what)>25):
    what=what[0:25]

   table += "| "+str(shortyear)+"-"+str(shortmon)+"-"+str(shortday)+" | ["+str(dict[i][0])+"|https://jira.lsstcorp.org/browse/"+str(dict[i][0])+"] | "+str(dict[i][1])+ "|{color:"+scolor+"}"+statstring+"{color} | [pDa|"+dict[i][3]+"] |"+str(what)+"|\n"

 return table

def dicttotable1(dict,sorton):

 dictheader=['Date','PREOPS','STATS','(T,Q,D,Fa,Sf)','PANDA','DESCRIP']

 table="||"
 for i in dictheader:
   table += str(i)+"||"
 table += "\n"
 
 for i in sorted(dict.keys(),reverse=True):
   pis=i.split("#")[0]
   #print("pis is:",pis)
   stepstring=dict[i][4] 
   stepstart=stepstring[0:5]
   #print("stepstart is:",stepstart)
   if(stepstart=="step1"):
     ts=i.split("#")[1]
     status=dict[i][2]
     nT=status[0]
     nFile=status[1]
     nFin=status[2]
     nFail=status[3]
     nSubF=status[4]
     statstring=str(nT)+","+str(nFile)+","+str(nFin)+","+str(nFail)+","+str(nSubF)
     scolor="black"
     if(nFail>0):
      scolor="red"
     if(nT==nFin+nSubF):
      scolor="black"
     if(nT==nFin):
      scolor="green"
     if(nFail==0 and nFile==0):
      scolor="blue"
  
     longdatetime=ts
     shortyear=str(longdatetime[0:4])
     shortmon=str(longdatetime[4:6])
     shortday=str(longdatetime[6:8])
     #print(shortyear,shortmon,shortday)
  
     what=dict[i][4]
     if(len(what)>25):
      what=what[0:25]
  
     table += "| "+str(shortyear)+"-"+str(shortmon)+"-"+str(shortday)+" | ["+str(dict[i][0])+"|https://jira.lsstcorp.org/browse/"+str(dict[i][0])+"] | "+str(dict[i][1])+ "|{color:"+scolor+"}"+statstring+"{color} | [pDa|"+dict[i][3]+"] |"+str(what)+"|\n"

 return table


def drpaddjobtosummary(first,ts,pissue,jissue,status,frontend,frontend1,backend):

  #print("loading backend table")

  secrets = netrc.netrc()
  username,account,password = secrets.authenticators('lsstjira')
  authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))

  backendissue=authenticated_jira.issue(backend)
  olddescription = backendissue.fields.description

  frontendissue=authenticated_jira.issue(frontend)
  frontendissue1=authenticated_jira.issue(frontend1)

  jissue=authenticated_jira.issue(jissue)
  jdesc=  jissue.fields.description
  jsummary=  jissue.fields.summary
  print("summary is",jsummary)
  (ts,status,hilow,pandalink,what)=parseissuedesc(jdesc,jsummary)
  print("new entry (ts,status,hilow,pandalink,step)",ts,status,hilow,pandalink,what)

  if(first==1):
   dict={}
  else:
   dict=json.loads(olddescription)

  if(first==2):
   print("removing PREOPS, DRP",str(pissue),str(jissue))
   for key,value in dict.items():
     #print("key",key,"value",value)
     if value[1] == str(jissue) and value[0]==str(pissue):
       print("removing one key with:",str(jissue),str(pissue))
       del dict[key]
       break
  else:
   dict[str(pissue)+"#"+str(ts)]=[str(pissue),str(jissue),status,pandalink,what+str(hilow)]

  newdesc=dicttotable(dict,-1)
  frontendissue.update(fields={'description': newdesc})

  newdesc1=dicttotable1(dict,-1)
  frontendissue1.update(fields={'description': newdesc1})

  newdict=json.dumps(dict)
  backendissue.update(fields={'description': newdict})
  print("Summary updated, see DRP-55 or DRP-53")

if __name__ == "__main__":
  numpar = len(sys.argv)
  print('numpar is',numpar)
  if(numpar<2 or numpar>4):
    print("usage: DRPAddJobToSummary.py PREOPS-YY DRP-XX [reset|remove]")
    print("PREOPS-YY is the campaign defining ticket, also in the butler output name")
    print("DRP-XX is the issue created to track ProdStat for this bps submit")
    print("if you run the command twice with the same entries, it is ok")
    print("if you specify remove, it will instead remove one entry from the table with the DRP/PREOPS number")
    print("if you specify reset is will erase the whole table (don't do this lightly)")
    print("To see the output summary:View special DRP tickets DRP-53 (all bps submits entered) and https://jira.lsstcorp.org/browse/DRP-55 (step1 submits only)")
    sys.exit(1)
  if(numpar>1):
    pissue=sys.argv[1]
  else:
    pissue="DRP0"
  if(numpar>2):
    drpi=sys.argv[2]
    
  first=0
  if(numpar>3 and sys.argv[3]=="reset"):
    print("resetting")
    first=1
  if(numpar>3 and sys.argv[3]=="remove"):
    print("removing")
    first=2
  frontend="DRP-53"
  frontend1="DRP-55"
  backend="DRP-54"
  ts="-1"
  status="-1"
  drpaddjobtosummary(first,ts,pissue,drpi,status,frontend,frontend1,backend)
