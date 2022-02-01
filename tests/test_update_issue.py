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

import os
import unittest
from unittest import mock
from tempfile import TemporaryDirectory
import netrc
import tarfile

from lsst.ProdStat import DRPUtils

TEST_DATA_FNAME = os.path.join(
    os.environ["PRODSTAT_DIR"], "tests", "data", "testdrp.tgz"
)

# Mock netrc.netrc on the class, using a pre-made MOCK_NETRC object,
# so that we only need to specify the return value for authenticators
# once, and there's no need for a different mock of this for different tests.

MOCK_NETRC = mock.Mock(netrc.netrc)
MOCK_NETRC.return_value.authenticators.return_value = (
    "test_username",
    "test_account",
    "test_password",
)


@mock.patch("netrc.netrc", MOCK_NETRC)
class TestUpdateIssue(unittest.TestCase):
    def setUp(self):
        self.start_dir = os.getcwd()
        self.temp_dir = TemporaryDirectory()
        with tarfile.open(TEST_DATA_FNAME) as data_tar:
            data_tar = tarfile.open(TEST_DATA_FNAME)
            data_tar.extractall(self.temp_dir.name)

        self.test_dir = os.path.join(self.temp_dir.name, "testdrp")
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.start_dir)
        self.temp_dir.cleanup()

    # Mock JiraUtils.JIRA on the test method, se we get different instances
    # of the mock in each test.

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
