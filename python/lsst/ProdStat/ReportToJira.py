#!/usr/bin/env python
# This file is part of ProdStat package.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import yaml
from .JiraUtils import JiraUtils

__all__ = ['ReportToJira']


class ReportToJira:
    """Report production statistics to a Jira ticket

    Parameters
    ----------
    inp_file : `str`
        Path to a yaml file that looks like following::

            project: 'Pre-Operations'
            Jira: PREOPS-911
            comments:
            - file: /Users/kuropat/devel/reports/pandaWfStat-PREOPS-911.txt
              tokens:
                 - 'pandaWfStat'
                 - 'workflow'
            - file: /Users/kuropat/devel/reports/pandaStat-PREOPS-911.txt
              tokens:
                 - 'pandaStat'
                 - 'campaign'
            - file: /home/kuropat/devel/reports/butlerStat-PREOPS-911_step1.txt
              tokens:
                - 'butlerStat'
                - 'Campaign'
            attachments:
              - /Users/kuropat/devel/reports/pandaWfStat-PREOPS-911.html
    """

    def __init__(self, inp_file):
        self.ju = JiraUtils()
        (self.a_jira, account) = self.ju.get_login()
        with open(inp_file) as pf:
            in_pars = yaml.safe_load(pf)
        self.ticket = in_pars['Jira']
        if 'comments' in in_pars:
            self.comments = in_pars['comments']
        else:
            self.comments = list()
        print(self.comments)
        if 'attachments' in in_pars:
            self.attachments = in_pars['attachments']
        else:
            self.attachments = list()
        print(self.attachments)
        self.project = in_pars['project']

    def run(self):
        """Update the jira ticket."""
        print("The summary for ticket:", self.ticket)
        issue_id = self.ju.get_issue_id(self.project, self.ticket)
        issue = self.ju.get_issue(self.ticket)
        summary = self.ju.get_summary(issue)
        print(summary)
        for comment in self.comments:
            com_file = comment['file']
            tokens = comment['tokens']
            str_buff = ''
            for line in open(com_file, 'r'):
                str_buff += line
            self.ju.update_comment(self.a_jira, self.ticket, issue_id, tokens, str_buff)
        for attachment in self.attachments:
            att_file = str(attachment)
            #            att_name = att_file.split('/')[-1]
            self.ju.update_attachment(self.a_jira, issue, att_file)
