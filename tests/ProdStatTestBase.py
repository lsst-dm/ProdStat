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
"""Base for tests of ProdStat functions."""

import os
from unittest import mock
from tempfile import TemporaryDirectory
import netrc
import tarfile

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
class ProdStatTestBase:
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
