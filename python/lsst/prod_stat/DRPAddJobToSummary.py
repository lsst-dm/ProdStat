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
from DRPUtils import *

if __name__ == "__main__":
    numpar = len(sys.argv)
    print("numpar is", numpar)
    if numpar < 2 or numpar > 4:
        print("usage: DRPAddJobToSummary.py PREOPS-YY DRP-XX [reset|remove]")
        print(
            "PREOPS-YY is the campaign defining ticket, also in the butler output name"
        )
        print("DRP-XX is the issue created to track ProdStat for this bps submit")
        print("if you run the command twice with the same entries, it is ok")
        print(
            "if you specify remove, it will instead remove one entry from the table with the DRP/PREOPS number"
        )
        print(
            "if you specify reset is will erase the whole table (don't do this lightly)"
        )
        print("To see the output summary:View special DRP tickets DRP-53 ")
        print("all bps submits entered) and https://jira.lsstcorp.org/browse/DRP-55")
        print("(step1 submits only)")
        sys.exit(1)
    if numpar > 1:
        pissue = sys.argv[1]
    else:
        pissue = "DRP0"
    if numpar > 2:
        drpi = sys.argv[2]

    first = 0
    if numpar > 3 and sys.argv[3] == "reset":
        print("resetting")
        first = 1
    if numpar > 3 and sys.argv[3] == "remove":
        print("removing")
        first = 2
    frontend = "DRP-53"
    frontend1 = "DRP-55"
    backend = "DRP-54"
    ts = "-1"
    status = "-1"
    drp = DRPUtils()
    drp.drp_add_job_to_summary(
        first, ts, pissue, drpi, status, frontend, frontend1, backend
    )
