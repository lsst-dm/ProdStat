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
"""Test add-job-to-summary."""

import json
import unittest
from unittest import mock

from lsst.ProdStat import DRPUtils
from ProdStatTestBase import ProdStatTestBase, MOCK_NETRC

TEST_ISSUE_SUMMARY = "step1#v23_0_0_rc5/PREOPS-973/20220127T205042Z"
TEST_ISSUE_DESCRIPTION = json.dumps(
    {
        "PREOPS-863#20211102T154605Z": [
            "PREOPS-863",
            "DRP-12",
            [7, 1727, 6, 1, 0],
            "https://panda-doma.cern.ch/tasks/?taskname=blah_blah_blah",
            "step2",
        ]
    }
)


@mock.patch("netrc.netrc", MOCK_NETRC)
class TestAddJobToSummary(ProdStatTestBase, unittest.TestCase):
    @mock.patch("lsst.ProdStat.JiraUtils.JIRA", autospec=True)
    def test_add_job_to_summary(self, MockJira):
        drp_issue = "DRP-YYY"
        production_issue = "PREOPS-XXX"
        frontend = "DRP-53"
        frontend1 = "DRP-55"
        backend = "DRP-54"
        first = 0

        test_issue_fields = MockJira.return_value.issue.return_value.fields
        test_issue_fields.summary = TEST_ISSUE_SUMMARY
        test_issue_fields.description = TEST_ISSUE_DESCRIPTION

        drp = DRPUtils.DRPUtils()
        drp.drp_add_job_to_summary(
            first, production_issue, drp_issue, frontend, frontend1, backend
        )

        mock_jira = MockJira.return_value

        self.assertIn(mock.call(drp_issue), mock_jira.issue.call_args_list)
        self.assertIn(mock.call(frontend), mock_jira.issue.call_args_list)
        self.assertIn(mock.call(frontend1), mock_jira.issue.call_args_list)
        self.assertIn(mock.call(backend), mock_jira.issue.call_args_list)

        mock_issue = mock_jira.issue.return_value
        self.assertEqual(mock_issue.update.call_count, 3)
        for update_args, update_kwargs in mock_issue.update.call_args_list:
            description = update_kwargs["fields"]["description"]
            self.assertIsInstance(description, str)
            self.assertGreater(len(description), 10)
