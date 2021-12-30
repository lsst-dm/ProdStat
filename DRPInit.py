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

# usage:  DRPInit.py bps_group_template.yaml PREOPS-XXXX [DRP-X]
#
if __name__ == "__main__":
    nbpar = len(sys.argv)
    if nbpar < 3:
        print("Usage: ")
        print(" DRPInit.py <bps_submit_yaml_template> <Production Issue> [DRP-issue]")
        print("  <bps_submit_yaml_template>:")
        print("      Template file with place holders for start/end ")
        print("       dataset/visit/tracts (will be attached to Production Issue)")
        print("  <Production Issue>: ")
        print("       Pre-existing issue of form PREOPS-XXX (later DRP-XXX) to update ")
        print("       with link to ProdStat tracking issue(s) -- should match issue in template keyword ")
        print("  [DRP-issue]: ")
        print("       If present in form DRP-XXX, redo by overwriting an ")
        print("       existing DRP-issue. If not present, create a new DRP-issue.")
        print("       All ProdStat plots and links for group of bps submits will be ")
        print("       tracked off this DRP-issue.  Production Issue will be updated with")
        print("       a link to this issue, by updating description (or later by using")
        print("        subtask link if all are DRP type). ")
        sys.exit(-2)

    template = sys.argv[1]
    pissue = sys.argv[2]
    drpi = "DRP0"
    if nbpar >= 4:
        drpi = sys.argv[3]
    else:
        drpi = "DRP0"
    drp = DRPUtils()
    drp.drp_init(template, pissue, drpi)
