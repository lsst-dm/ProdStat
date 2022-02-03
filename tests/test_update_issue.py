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
# coding: utf-8
"""Test update-issue."""

import unittest
from unittest import mock

from lsst.ProdStat import DRPUtils
from ProdStatTestBase import ProdStatTestBase, MOCK_NETRC


@mock.patch("netrc.netrc", MOCK_NETRC)
class TestUpdateIssue(ProdStatTestBase, unittest.TestCase):
    @mock.patch("lsst.ProdStat.JiraUtils.JIRA", autospec=True)
    def test_new_issue(self, MockJira):
        drp_issue = "DRP0"
        production_issue = "PREOPS-XXX"
        ts = "0"
        bps_submit_fname = "clusttest_all_1.yaml"

        drp = DRPUtils.DRPUtils()

        drp.drp_issue_update(bps_submit_fname, production_issue, drp_issue, ts)

        mock_jira = MockJira.return_value
        mock_jira.create_issue.assert_called_once()
        create_issue_args, create_issue_kwargs = mock_jira.create_issue.call_args
        self.assertTupleEqual(create_issue_args, ())
        self.assertEqual(
            set(create_issue_kwargs),
            set(["project", "issuetype", "summary", "description", "components"]),
        )

    @mock.patch("lsst.ProdStat.JiraUtils.JIRA", autospec=True)
    def test_update_existing_issue(self, MockJira):
        drp_issue = "DRP-148"
        production_issue = "PREOPS-XXX"
        ts = "0"
        bps_submit_fname = "clusttest_all_1.yaml"

        drp = DRPUtils.DRPUtils()

        drp.drp_issue_update(bps_submit_fname, production_issue, drp_issue, ts)

        mock_jira = MockJira.return_value

        mock_jira.create_issue.assert_not_called()

        issue_args, issue_kwargs = mock_jira.issue.call_args
        self.assertEqual(issue_args, (drp_issue,))
        self.assertEqual(issue_kwargs, {})

        issue_update = mock_jira.issue.return_value.update
        update_args, update_kwargs = issue_update.call_args
        self.assertTupleEqual(update_args, ())
        self.assertEqual(set(update_kwargs.keys()), set(["fields"]))
        self.assertEqual(
            set(update_kwargs["fields"].keys()), set(["summary", "description"])
        )
