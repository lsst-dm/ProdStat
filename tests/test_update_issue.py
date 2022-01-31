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

# import jira

import lsst.ProdStat
import lsst.ProdStat.JiraUtils
from lsst.ProdStat import DRPUtils

TEST_DATA_FNAME = os.path.join(
    os.environ["PRODSTAT_DIR"], "tests", "data", "testdrp.tgz"
)

MOCK_NETRC = mock.Mock(netrc.netrc)
MOCK_NETRC.return_value.authenticators.return_value = (
    "test_username",
    "test_account",
    "test_password",
)

# MOCK_JIRO_PROPERTYHOLDER = mock.Mock(jira.resources.PropertyHolder)
# MOCK_JIRO_PROPERTYHOLDER.return_value.description = "Test description str."

# MOCK_ISSUE = mock.Mock(jira.resources.Issue)
# MOCK_ISSUE.return_value.fields = MOCK_JIRO_PROPERTYHOLDER()

MOCK_JIRA = mock.Mock(lsst.ProdStat.JiraUtils.JIRA)
# MOCK_JIRA.return_value.create_issue.return_value = MOCK_ISSUE()


@mock.patch("netrc.netrc", MOCK_NETRC)
@mock.patch("lsst.ProdStat.JiraUtils.JIRA", MOCK_JIRA)
class TestUpdateIssue(unittest.TestCase):
    def setUp(self):
        pass

    def test_new_issue(self):
        drp_issue = "DRP0"
        production_issue = "PREOPS-XXX"
        ts = "0"

        drp = DRPUtils.DRPUtils()

        with TemporaryDirectory() as base_test_dir:
            with tarfile.open(TEST_DATA_FNAME) as data_tar:
                data_tar = tarfile.open(TEST_DATA_FNAME)
                data_tar.extractall(base_test_dir)

            test_dir = os.path.join(base_test_dir, "testdrp")
            start_dir = os.getcwd()
            os.chdir(test_dir)

            bps_submit_fname = "clusttest_all_1.yaml"

            drp.drp_issue_update(bps_submit_fname, production_issue, drp_issue, ts)

            os.chdir(start_dir)

        mock_jira = MOCK_JIRA.return_value
        mock_jira.create_issue.assert_called_once()

