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
"""Test make-prod-groups."""

import unittest
import os
import re
from os import path
from tempfile import TemporaryDirectory
import yaml
import numpy as np
import numpy.random

from lsst.ProdStat.DRPUtils import DRPUtils

RANDOM_SEED = 6563
NUM_EXPOSURES = 1000

TEMPLATE1 = """includeConfigs:
- ${CTRL_BPS_DIR}/config/bps_idf.yaml
- requestMemory.yaml
- clustering.yaml

project: dp02
campaign: v23_0_0_rc5/PREOPS-938
pipelineYaml: "${OBS_LSST_DIR}/pipelines/imsim/DRP.yaml#step1"

payload:
  payloadName: 2.2i/all_band_GNUM
  output: "{payloadName}/{campaign}"
  butlerConfig: s3://butler-us-central1-panda-dev/dc2/butler-external.yaml
  inCollection: "2.2i/defaults"
  dataQuery: "instrument='LSSTCam-imSim' and skymap='DC2' and exposure >=LOWEXP and exposure <= HIGHEXP"
  sw_image: "lsstsqre/centos:7-stack-lsst_distrib-v23_0_0_rc5"
"""


class TestMakeProdGroups(unittest.TestCase):
    """Tests for MakeProdGroups."""

    def setUp(self):
        """Prepare input files for testing."""
        # Create a random number generator with a specific seed to make the
        # test repeatable.
        self.start_cwd = os.getcwd()
        self.random_generator = numpy.random.default_rng(RANDOM_SEED)

        # Create the file from which to read the template
        self.in_dir = TemporaryDirectory()

        os.chdir(self.in_dir.name)

        self.template = path.join(self.in_dir.name, "clusttest1.yaml")
        with open(self.template, "w") as template_fp:
            template_fp.write(TEMPLATE1)

        # Create the file with the list of exporuses to group
        self.explist = path.join(self.in_dir.name, "explist")
        self.exp_ids = self.random_generator.choice(
            np.arange(2 * NUM_EXPOSURES),
            NUM_EXPOSURES,
            replace=False,
        )
        self.exp_ids.sort()
        self.bands = self.random_generator.choice(
            np.array(tuple("ugrizy")),
            NUM_EXPOSURES,
        )
        np.savetxt(
            self.explist, np.rec.fromarrays((self.bands, self.exp_ids)), fmt="%s %i"
        )

    def test_make_prod_groups(self):
        """Run the test of make-prod-groups."""
        band = "g"
        groupsize = 10
        skipgroups = 2
        ngroups = 2
        DRPUtils.make_prod_groups(
            self.template, band, groupsize, skipgroups, ngroups, self.explist
        )

        with open(self.template, "r") as in_file:
            in_params = yaml.safe_load(in_file)

        for group in np.arange(skipgroups + 1, skipgroups + ngroups + 1):
            out_fname = f"{path.splitext(self.template)[0]}_{band}_{group}.yaml"
            with open(out_fname, "r") as out_file:
                out_params = yaml.safe_load(out_file)

            # Spot check that things that should be unchaged are
            self.assertListEqual(
                in_params["includeConfigs"], out_params["includeConfigs"]
            )

            for keyword in ("project", "campaign", "pipelineYaml"):
                self.assertEqual(in_params[keyword], out_params[keyword])

            in_payload = in_params["payload"]
            out_payload = out_params["payload"]
            for keyword in ("output", "butlerConfig", "inCollection", "sw_image"):
                self.assertEqual(in_payload[keyword], out_payload[keyword])

            # Check that the group number got appropriately replaced
            self.assertEqual(
                in_payload["payloadName"].replace("GNUM", f"{group:d}"),
                out_payload["payloadName"],
            )

            # Make sure the exposure limits match what we requested
            exp_re = re.compile(
                r" *exposure *>= *(?P<lowexp>\d+) +and +exposure *<= *(?P<highexp>\d+)"
            )
            matched_query = exp_re.search(out_payload["dataQuery"])
            low_exp = int(matched_query.group("lowexp"))
            high_exp = int(matched_query.group("highexp"))

            # Check that the group starts at the right exposure
            exp_below = np.count_nonzero(
                (self.exp_ids < low_exp) & (self.bands == band)
            )
            self.assertEqual((group - 1) * groupsize, exp_below)

            # Check that we have the right number of exposures in the group
            query_exps = np.count_nonzero(
                (self.exp_ids >= low_exp)
                & (self.exp_ids <= high_exp)
                & (self.bands == band)
            )
            self.assertEqual(groupsize, query_exps)

    def tearDown(self):
        os.chdir(self.start_cwd)
        self.in_dir.cleanup()
