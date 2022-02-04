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
"""Test get-butler-stat."""

import os
import unittest
from unittest import mock
import gzip
import json

from lsst.ProdStat.GetButlerStat import GetButlerStat

TEST_BUTLER_STAT_PARAM_FNAME = os.path.join(
    os.environ["PRODSTAT_DIR"], "tests", "data", "get_butler_stat_params.json"
)


class TestGetButlerStat(unittest.TestCase):

    @mock.patch("lsst.ProdStat.GetButlerStat.plt.show")
    def test_get_butler_stat(self, mock_plt_show):
        with open(TEST_BUTLER_STAT_PARAM_FNAME, "rt", encoding="UTF-8") as param_io:
            get_butler_stat_kwargs = json.load(param_io)

        get_butler_stat = GetButlerStat(**get_butler_stat_kwargs)
        get_butler_stat.run()
