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
"""Test get-panda-stat."""

import os
import unittest
from unittest import mock
import gzip
import json

from lsst.ProdStat.GetPanDaStat import GetPanDaStat

TEST_PANDA_QUERY_FNAME = os.path.join(
    os.environ["PRODSTAT_DIR"], "tests", "data", "panda_query_results.json.gz"
)

TEST_PANDA_STAT_PARAM_FNAME = os.path.join(
    os.environ["PRODSTAT_DIR"], "tests", "data", "get_panda_stat_params.json"
)


def _mock_querypanda(self, urlst):
    fname = TEST_PANDA_QUERY_FNAME
    this_open = gzip.open if fname.endswith(".gz") else open
    try:
        with this_open(fname, "rt", encoding="UTF-8") as in_io:
            captured = json.load(in_io)
    except FileNotFoundError:
        captured = {}

    result = captured[urlst]

    return result


class TestGetPandaStat(unittest.TestCase):

    # Patch to replace calles to GetPanDaStat.querypanda with the function
    # to read data from the test json file.
    @mock.patch.object(GetPanDaStat, "querypanda", new=_mock_querypanda)
    @mock.patch("lsst.ProdStat.GetPanDaStat.plt.show")
    def test_get_panda_stat(self, mock_plt_show):
        with open(TEST_PANDA_STAT_PARAM_FNAME, "rt", encoding="UTF-8") as param_io:
            get_panda_stat_kwargs = json.load(param_io)

        get_panda_stat = GetPanDaStat(**get_panda_stat_kwargs)
        get_panda_stat.run()
