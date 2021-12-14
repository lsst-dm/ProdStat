#!/usr/bin/env python
"""The program to put comments and attachments to a Jira ticker"""
import netrc
from jira import JIRA
import argparse
import getpass
import keyring
import datetime

class JiraUtils:
    def __init__(self):
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators('lsstjira')

    def get_login(self):
        """
        tries to get user info from ~/.netrc
        :return: (jira instance, username, password), tuple
        """
        secrets = netrc.netrc()
        username, account, password = secrets.authenticators('lsstjira')
        self.ajira = JIRA(options={'server': account}, basic_auth=(username, password))
        return self.ajira, username, password

    def get_issue(self, ticket):
        """
        return issue object for given ticket
        :param ticket: issue key, string
        :return: issue instance
        """
        issue_object = self.ajira.issue(ticket)
        return issue_object

    def get_issueId(self, project, key):
        """
        return issueID having project like 'Pre-Operetions' and key like 'PREOPS-910'
        :param project: project name, string
        :param key: issue key, string
        :return: issueId, string
        """
        jql_str = "project=%s AND issue=%s " % (project, key)
        query = self.ajira.search_issues(
            jql_str=jql_str
        )
        print("query=", query)
        issueId = int(query[0].id)
        print("Issue id=", issueId)
        return issueId

    def get_comments(self, issue):
        """
        Creates dictionary of comments with keys = commentId
        :param issue: issue instance
        :return: dictionary commentID:comment['body']
        """
        all_comments = dict()
        for field_name in issue.raw['fields']:
            if "comment" in field_name:
                comments = issue.raw['fields'][field_name]
                com_list = comments['comments']
                print("comments")
                for comment in com_list:
                    all_comments[comment["id"]] = comment["body"]
        return all_comments

    def get_attachments(self,issue):
        """
        select all attachments in the issue and return dictionary of attachmentId: filename
        :param issue: issue instance
        :return: dictionary issueId:issue.filename
        """
        all_attachments = dict()
        for field_name in issue.raw['fields']:
            if "attachment" in field_name:
                attachments = issue.raw['fields'][field_name]
                for attachment in attachments:
                    all_attachments[attachment["id"]] = attachment["filename"]
                    print(attachment["id"], ' ', attachment["filename"])
        return all_attachments

    def get_summary(self, issue):
        """
        return summary of given issue
        :param issue: issue instance
        :return: summary
        """
        summary = issue.fields.summary
        for field_name in issue.raw['fields']:
            print("Field:", field_name, "Value:", issue.raw['fields'][field_name])
        return summary

    def add_attachment(self, jira, issue, attFile):
        """
        Add attachment file to an issue
        :param jira:
        :param issue:
        :param attFile:
        :return:
        """
        jira.add_attachment(issue=issue, attachment=attFile)


    """ creat issue with parameters in the issue dictionary
     that can look like:
     issue_fields = {
    "summary": "Learn Python",
    "description": "Remember to study up on Python programming",
    "project": project_field,
    "issuetype": issue_type_field
    }
    """
    def create_issue(self, jira, issue_dict):
        """
        Create an issue
        :param jira:
        :param issue_dict:
        :return: issue instance
        """
        new_issue = jira.create_issue(fields=issue_dict)
        return new_issue

    def update_issue(self, issue, issue_dict):
        """
        Update some or all issue fields
        :param issue:
        :param issue_dict:
        :return:
        """
        issue.update(fields=issue_dict)

    def add_comment(self, issue, worklog):
        """
        Add comment to an issue
        :param issue: issue instance
        :param worklog: dictionary containing:
              'author': author name, string
              'created': timestamp, string
               'comment': comment body, string
        :return:
        """
        issue.update(comment=worklog['comment'])

    def update_comment(self, jira, key, issueId, tokens, commentS):
        """
        Update comment replacing its body
        :param jira: api instance
        :param key:  issue key, string
        :param tokens: tokens to identify comment, list of strings
        :param commentS: comment body, string
        :return:
        """
        issue = self.get_issue(key)
        all_comments = self.get_comments(issue)
        if len(all_comments) != 0:
            for id in all_comments:
                commStr = self.get_comment(jira, issueId, id)
                print('Id:', id, " comment:", commStr)
                comment = jira.comment(key, id)  # with key and comment id
                print("Found comment:",commStr)
                found = False
                for token in tokens:
                    if token in commStr:
                        found = True
                    else:
                        found = False
                        break
                if found:
                    comment.update(body=commentS)
        else:
            worklog = dict()
            worklog['author'] = 'Nikolay Kuropatkin'
            tstamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            worklog['created'] = tstamp
            worklog['comment'] = commentS
            self.add_comment(issue, worklog)

    def get_comment(self, jira, issueId, commentId):
        """
        Return comment body identified by issueID and commentId
        :param jira: jira API instance
        :param issueId: issue id, string
        :param commentId: comment id, string
        :return:
        """
        comStr = jira.comment(int(issueId), int(commentId)).body
        return comStr

    def update_attachment(self, jira, issue, attFile):
        """
        to replace attachment in an issue we first delete one containing
        selected filename and then add a new one
        :param jira: jira API instance
        :param issue: issue instance
        :param attFile: file /path/name, string
        :return:
        """
        attachments = issue.raw['fields']['attachment']
        print("attachments ", attachments)
        if len(attachments) != 0:
            for attachment in attachments:
                print('attachment:', attachment['id'], ' ', attachment['filename'])
                attId = attachment['id']
                filename = attachment['filename']
                if filename in attFile:
                    print(" Found attachment id ", attachment['id'])
                    jira.delete_attachment(int(attId))
                    self.add_attachment(jira, issue, attFile)
        else:
            self.add_attachment(jira, issue, attFile)

    def get_description(self, issue):
        """
        read issue description
        :param issue: issue instance
        :return: description, string
        """
        description = issue.raw['fields']['description']
        return description

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-t",
        "--ticket",
        default='PREOPS-728',
        help="Specify Jira ticket")

    options = parser.parse_args()
    ticket = options.ticket
    print("ticket=", ticket)
    ju = JiraUtils()
    jira, username, password = ju.get_login()
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
    issueId = ju.get_issueId(project='DRP', key=ticket)
    print("issueId=",issueId)
    desc = ju.get_description(issue)
    print("desc:", desc)
    " Let's make attachment if not exists"
    attFile = './table.html'
    ju.update_attachment( jira, issue, attFile)
    print("issue fields attachment:", issue.fields.attachment)
    " Now create or update a comment"
    commentS = """ The test comment for pandaStat and PREOPS-910"""
    tokens = ['pandaStat', 'PREOPS-910']
    ju.update_comment(jira, ticket, issueId, tokens, commentS)
    commentS = """ New comment for pandaStat and PREOPS-910"""
    ju.update_comment(jira, ticket, issueId, tokens, commentS)
    return


if __name__ == "__main__":
    main()