#!/usr/bin/env python
# This script shows how to use the client in anonymous mode
# against jira.atlassian.com.
import re
import netrc
from jira import JIRA
from jira.client import ResultList
from jira.resources import Issue
#jira_options = {'server' : 'https://jira.lsstcorp.org'}
secrets = netrc.netrc()
username,account,password = secrets.authenticators('lsstjira')
authenticated_jira = JIRA(options={'server': account}, basic_auth=(username, password))
issue=authenticated_jira.issue("PREOPS-607")
print(list(issue.fields.comment.comments))
summary = issue.fields.summary
print(summary)
print(issue.fields.attachment)