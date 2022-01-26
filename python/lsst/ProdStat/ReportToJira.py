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
import click
from .JiraUtils import *

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
            - file: /Users/kuropat/devel/reports//butlerStat-PREOPS-911_step1.txt
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
        self.comments = in_pars['comments']
        print(self.comments)
        self.attachments = in_pars['attachments']
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


@click.group()
def cli():
    """Command line interface for ReportToJira program"""
    pass


@cli.command()
@click.argument("param_file", type=click.Path(exists=True))
def report(param_file):
    """Report production statistics to a Jira ticket

    Parameters
    ----------
    param param_file: `str`
        name of the parameter yaml file with path

    Notes
    -----
    The yaml file should provide following parameters:

    \b
    project: 'Pre-Operations'
        project name
    Jira: `str`
        jira ticket like PREOPS-905
    comments: `list`
        list of comment files with path
        each file entry contains list of tokens to identify comment
        to be replaced
    attachments: `list`
        list of attachment files with path
    :return:
    """
    click.echo("Start with ReportToJira")
    report_to_jira = ReportToJira(param_file)
    report_to_jira.run()
    print("End with ReportToJira")


if __name__ == "__main__":
    cli()
