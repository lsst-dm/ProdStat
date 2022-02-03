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
"""Test update-stat."""

import unittest
from unittest import mock

from lsst.ProdStat import DRPUtils
from ProdStatTestBase import ProdStatTestBase, MOCK_NETRC

TEST_ISSUE_SUMMARY = "step1#v23_0_0_rc5/PREOPS-973/20220127T205042Z"


@mock.patch("netrc.netrc", MOCK_NETRC)
class TestUpdateStat(ProdStatTestBase, unittest.TestCase):
    @mock.patch("lsst.ProdStat.DRPUtils.GetButlerStat", autospec=True)
    @mock.patch("lsst.ProdStat.DRPUtils.GetPanDaStat", autospec=True)
    @mock.patch("lsst.ProdStat.JiraUtils.JIRA", autospec=True)
    def test_update_stat(self, MockJira, MockGetPanDaStat, MockGetButlerStat):
        drp_issue = "DRP-YYY"
        production_issue = "PREOPS-XXX"

        test_issue_fields = MockJira.return_value.issue.return_value.fields
        test_issue_fields.summary = TEST_ISSUE_SUMMARY

        drp = DRPUtils.DRPUtils()
        drp.drp_stat_update(production_issue, drp_issue)
        mock_jira = MockJira.return_value
        self.assertEqual(mock_jira.issue.call_args_list, [mock.call(drp_issue)])

        mock_issue = mock_jira.issue.return_value
        mock_issue.update.assert_called_once()

        MockGetButlerStat.assert_called_once()
        MockGetButlerStat.return_value.run.assert_called_once()

        MockGetPanDaStat.assert_called_once()
        MockGetPanDaStat.return_value.run.assert_called_once()
