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
# NOTES:  allow for clustering, and allow for appending to the DRP ticket description
import sys
from .DRPUtils import DRPUtils

if __name__ == "__main__":
    nbpar = len(sys.argv)
    if nbpar < 2:
        print("Usage: ")
        print(
            "   DRPStatUpate.py <bps_submit_yaml> [Production Issue] [DRP Issue(toredo)]"
        )
        print("  <bps_submit_yaml>: ")
        print(
            "       yaml file used with bps submit <bps_submit_yaml> .  Should be sitting "
        )
        print(
            "       in the same dir that bps submit was done, so that the submit/ dir can be"
        )
        print("       searched for more info")
        print("  [Production Issue]:")
        print(
            "       PREOPS-938 or similar production issue for this group of bps submissions"
        )
        print("  [DRP Issue]: ")
        print(
            "        leave off if you want a new issue generated, to redo, include the DRP-issue generated last time"
        )
        sys.exit(-2)

    if nbpar > 1:
        pissue = sys.argv[1]
    else:
        pissue = "PREOPS0"

    if nbpar > 2:
        drpi = sys.argv[2]
    else:
        drpi = "DRP0"
    drp_utils = DRPUtils()
    drp_utils.drp_stat_update(pissue, drpi)
