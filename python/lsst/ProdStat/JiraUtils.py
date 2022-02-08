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
import netrc
from jira import JIRA
import argparse
import datetime

__all__ = ["JiraUtils"]


class JiraUtils:
    def __init__(self):
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators("lsstjira")
        self.ajira = JIRA(options={"server": account}, basic_auth=(username, password))
        self.user_name = username

    def get_login(self):
        """Tries to get user info from ~/.netrc

        Returns
        -------
        ajira : `jira.client.JIRA`
            Interface to JIRA.
        user_name : `str`
            The jira account being used.
        """
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators("lsstjira")
        self.user_name = username
        self.ajira = JIRA(options={"server": account}, basic_auth=(username, password))
        return self.ajira, self.user_name

    def get_issue(self, ticket):
        """Return issue object for given ticket.

        Parameters
        ----------
        ticket : `str`
            The name of the issue to get.

        Returns
        -------
        issue_object : `jira.resouce.Issue`
            The object representing the content of the issue.
        """
        issue_object = self.ajira.issue(ticket)
        return issue_object

    def get_issue_id(self, project, key):
        """Get issueID with requested project and key.

        Parameters
        ----------
        project : `str`
            project name
        key : `str`
            issue key

        Returns
        -------
        issueId : `str`
            the issue ID
        """
        jql_str = "project=%s AND issue=%s " % (project, key)
        query = self.ajira.search_issues(jql_str=jql_str)
        print("query=", query)
        issue_id = int(query[0].id)
        print("Issue id=", issue_id)
        return issue_id

    @staticmethod
    def get_comments(issue):
        """Creates dictionary of comments with keys = commentId.

        Parameters
        ----------
        issue : `jira.resource.Issue`
            issue instance

        Returns
        -------
        all_comments
            commentID:comment['body'] (`dict`)
        """
        all_comments = dict()
        for field_name in issue.raw["fields"]:
            if "comment" in field_name:
                comments = issue.raw["fields"][field_name]
                com_list = comments["comments"]
                print("comments")
                for comment in com_list:
                    all_comments[comment["id"]] = comment["body"]
        return all_comments

    @staticmethod
    def get_attachments(issue):
        """Select all issue attachments and get dict of attachmentId: filename.

        Parameters
        ----------
        issue : `jira.resource.Issue`
            issue instance

        Returns
        -------
        all_attachments : `dict`
            issueId: issue.filename
        """

        all_attachments = dict()
        for field_name in issue.raw["fields"]:
            if "attachment" in field_name:
                attachments = issue.raw["fields"][field_name]
                for attachment in attachments:
                    all_attachments[attachment["id"]] = attachment["filename"]
                    print(attachment["id"], " ", attachment["filename"])
        return all_attachments

    @staticmethod
    def get_summary(issue):
        """Return summary of given issue.

        Parameters
        ----------
        issue : `jira.resource.Issue`
            the issue from which to get the summary

        Returns
        -------
        summary : `str`
            The summary.
        """

        summary = issue.fields.summary
        for field_name in issue.raw["fields"]:
            print("Field:", field_name, "Value:", issue.raw["fields"][field_name])
        return summary

    @staticmethod
    def add_attachment(jira, issue, att_file):
        """Add attachment file to an issue.

        Parameters
        ----------
        jira : `jira.client.JIRA`
            jira instance
        issue : `jira.resource.Issue`
            issue instance
        """
        jira.add_attachment(issue=issue, attachment=att_file)

    """ creat issue with parameters in the issue dictionary
     that can look like:
     issue_fields = {
    "summary": "Learn Python",
    "description": "Remember to study up on Python programming",
    "project": project_field,
    "issuetype": issue_type_field
    }
    """

    @staticmethod
    def create_issue(jira, issue_dict):
        """Create an issue.

        Parameters
        ----------
        jira : `jira.client.JIRA`
            jira instance
        issue_dict : `dict`
            dictionary with issue fields:

            ``"summary"``
                issue summary (`str`)
            ``"description"``
                issue description (`str`)
            ``"project"``
                issue project (`str`)
            ``"issuetype"``
                issue type (`str`)

        Returns
        -------
        new_issue : `jira.resource.Issue`
            issue instance
        """
        new_issue = jira.create_issue(fields=issue_dict)
        return new_issue

    @staticmethod
    def update_issue(issue, issue_dict):
        """Update some or all issue fields.

        Parameters
        ----------
        issue : `jira.resource.Issue`
            issue instance
        issue_dict : `dict`
            dictionary with issue fields
        """
        issue.update(fields=issue_dict)

    @staticmethod
    def add_comment(issue, work_log):
        """Add comment to an issue.

        Parameters
        ----------
        issue : `jira.resource.Issue`
            issue instance
        work_log : `dict`
            A work log dictionary with:

            ``"author"``
                auther name (`str`)
            ``"created"``
                timestamp (`str`)
            ``"comment"``
                comment string (`str`)
        """
        issue.update(comment=work_log["comment"])

    def update_comment(self, jira, key, issue_id, tokens, comment_s):
        """Update comment replacing its body.

        Parameters
        ----------
        jira : `jira.client.JIRA`
            api instance
        key : `str`
            issue key
        token : `str`
            issue token
        comment_s : `str`
            comment body
        issue_id : `str`
            issue id
        """

        issue = self.get_issue(key)
        all_comments = self.get_comments(issue)
        if len(all_comments) > 0:
            updated = False
            for sid in all_comments:
                comm_str = self.get_comment(jira, issue_id, sid)
                comment = jira.comment(key, sid)  # with key and comment id
                found = False
                for token in tokens:
                    if token in comm_str:
                        found = True
                    else:
                        found = False
                        break
                if found:
                    comment.update(body=comment_s)
                    updated = True
            if not updated:
                "if not found comment to update add a new one"
                work_log = dict()
                work_log["author"] = self.user_name
                t_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                work_log["created"] = t_stamp
                work_log["comment"] = comment_s
                self.add_comment(issue, work_log)

        else:
            """if no comments create a new one"""
            work_log = dict()
            work_log["author"] = self.user_name
            t_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            work_log["created"] = t_stamp
            work_log["comment"] = comment_s
            self.add_comment(issue, work_log)

    @staticmethod
    def get_comment(jira, issue_id, comment_id):
        """Return comment body identified by issueID and commentId.

        Parameters
        ----------
        jira : `jira.client.JIRA`
            jira API instance
        issue_id : `str`
            issue id
        comment_id : `str`
            comment id

        Returns
        -------
        com_str : `str`
            The contents of the comment.
        """

        com_str = jira.comment(int(issue_id), int(comment_id)).body
        return com_str

    def update_attachment(self, jira, issue, att_file):
        """Replate an attachment in an issue.

        Parameters
        ----------
        jira : `jira.client.JIRA`
            jira API instance
        issue : `jira.resource.Issue`
            issue instance
        att_file : `str`
            file /path/name

        Notes
        -----

        To replace attachment in an issue we first delete one containing
        selected filename and then add a new one
        """
        attachments = issue.raw["fields"]["attachment"]
        if len(attachments) != 0:
            found = False
            for attachment in attachments:
                print("attachment:", attachment["id"], " ", attachment["filename"])
                att_id = attachment["id"]
                filename = attachment["filename"]
                if filename in att_file:
                    found = True
                    jira.delete_attachment(int(att_id))
                    self.add_attachment(jira, issue, att_file)
            if not found:
                self.add_attachment(jira, issue, att_file)
        else:
            self.add_attachment(jira, issue, att_file)

    @staticmethod
    def get_description(issue):
        """Read issue description.

        Parameters
        ----------
        issue : `jira.resource.Issue`
            issue instance

        Returns
        -------
        description
            the description (`str`)
        """
        description = issue.raw["fields"]["description"]
        return description


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-t", "--ticket", default="PREOPS-728", help="Specify Jira ticket"
    )

    options = parser.parse_args()
    ticket = options.ticket
    print("ticket=", ticket)
    ju = JiraUtils()
    jira, username = ju.get_login()
    """ We will not create issue because we can not delete one
    issue_fields = {
        "summary": "Test Ticket",
        "description": "The ticket to test JitaUtils",
        "project": "DRP",
        "issuetype": {"name": "Task"},
        "components": [{"name": "Test"}]
    }
    issue = ju.create_issue( jira, issue_fields)
    key = issue.key
    if key is None:
        print("Failed to create ticket")
    else:
        print("Created new ticket, key=", key)
    " Now lets update the issue "
    issue_fields = {
        "summary": "New ticket summary"
    }
    issue = jira.issue(key)
    ju.update_issue(issue, issue_fields)
    summary = issue.fields.summary
    if 'New' in summary:
        print("succesfully updated ticket")
    else:
        print(" summary update failed")
    " Now lets delete the ticket"
    try:
        issue.delete()
        issue = jira.issue(key)
        if issue is None:
            print(" The issue was deleted")
        else:
            print("Failed to delete issue")
    except:
        print("Failed to delete issue")
    """
    issue = ju.get_issue(ticket)
    issue_id = ju.get_issue_id(project="DRP", key=ticket)
    print("issueId=", issue_id)
    desc = ju.get_description(issue)
    print("desc:", desc)
    " Let's make attachment if not exists"
    att_file = "./table.html"
    ju.update_attachment(jira, issue, att_file)
    print("issue fields attachment:", issue.fields.attachment)
    " Now create or update a comment"
    comment_s = """ The test comment for pandaStat and PREOPS-910"""
    tokens = ["pandaStat", "PREOPS-910"]
    ju.update_comment(jira, ticket, issue_id, tokens, comment_s)
    comment_s = """ New comment for pandaStat and PREOPS-910"""
    ju.update_comment(jira, ticket, issue_id, tokens, comment_s)
    return


if __name__ == "__main__":
    main()
