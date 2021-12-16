#!/usr/bin/env python
# This script shows how to use the client in anonymous mode
# against jira.atlassian.com.
import json
import sys
import re
import netrc
from jira import JIRA
from jira.client import ResultList
from jira.resources import Issue

def dicttotable(dict,dictheader,sorton):

 table="||"
 for i in dictheader:
   table += str(i)+"||"
 table += "\n"
 
 for i in sorted(dict.keys(),reverse=True):
   table += "| "+i+" | "+dict[i][0]+" | "+dict[i][1]+" | "+dict[i][2]+" | "+dict[i][3]+" |"+"\n"

 return table

#jira_options = {'server' : 'https://jira.lsstcorp.org'}
secrets = netrc.netrc()
username,account,password = secrets.authenticators('lsstjira')
authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))
issue=authenticated_jira.issue("DRP-51")
print("comments:")
print(list(issue.fields.comment.comments))
summary = issue.fields.summary
print("summary:")
print(summary)
print("attachment:")
print(issue.fields.attachment)
description = issue.fields.description
print("description:")
print(description)

#for field_name in issue.raw['fields']:
   #print("f:",field_name,issue.raw['fields'][field_name])

dict={}
dict={'20211005T121212Z':['DRP-33','[PREOPS-707|https://jira.lsstcorp.org/browse/PREOPS-707]','{color:green}D{color}','test rc2'],
'20211106T121212Z':['DRP-34','[PREOPS-728|https://jira.lsstcorp.org/browse/PREOPS-728]','{color:green}D{color}','test rc3'],
'20211207T121212Z':['DRP-51','[PREOPS-938|https://jira.lsstcorp.org/browse/PREOPS-938]','{color:black}R{color}','test rc5'],
'20211208T121212Z':['DRP-51','[PREOPS-938|https://jira.lsstcorp.org/browse/PREOPS-910]','{color:red}F{color}','test rc5'],
'20211103T121212Z':['DRP-29','[PREOPS-863|https://jira.lsstcorp.org/browse/PREOPS-863]','{color:green}D{color}','test rc4']}
dictheader=['START','DISSUE','PISSUE','STATUS','DESCRIPTION']


r=json.dumps(dict)
newstuff=dicttotable(dict,dictheader,-1)

if(len(sys.argv)> 1):
 issue.update(fields={'description': description+"\n"+newstuff+"\n"})

description = issue.fields.description
print("description:")
print(description)


