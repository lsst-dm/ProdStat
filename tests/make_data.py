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
"""Create data files used for testing."""

import os
import json
import gzip
from unittest.mock import patch

import click

from lsst.ProdStat.GetPanDaStat import GetPanDaStat

__all__ = ["main"]


@click.group()
def cli():
    """Command line interface creating ProdStat test data files."""


@cli.command()
@click.argument(
    "fname",
    default="panda_query_results.json.gz",
    type=str,
)
@click.argument(
    "param_fname",
    default="get_panda_stat_params.json",
    type=str,
)
def get_panda_query_results(fname, param_fname):
    """Get results of panda queries for GetPanDaStat testing.

    \b
    Parameters
    ----------
    fname : `str`
        File in which to store query results.
    param_fname : `str`
        File with arguments for GetPanDaStat constructor.
    """

    with open(param_fname, "rt", encoding="UTF-8") as param_io:
        get_panda_stat_kwargs = json.load(param_io)

    try:
        os.remove(fname)
    except FileNotFoundError:
        pass

    # Store a reference to the original querypanda so we can run it
    # after it gets patched.
    orig_querypanda = GetPanDaStat.querypanda

    # Create a replacement for querypanda that runs the old querypanda,
    # then stores the result in the results file.
    def capture_querypanda(self, urlst):
        result = orig_querypanda(urlst)
        this_open = gzip.open if fname.endswith(".gz") else open
        try:
            with this_open(fname, "rt", encoding="UTF-8") as in_io:
                captured = json.load(in_io)
        except FileNotFoundError:
            captured = {}

        captured[urlst] = result

        with this_open(fname, "wt", encoding="UTF-8") as out_io:
            json.dump(captured, out_io, indent=4)

        return result

    # Run GetPanDaStat, patched with our new querypanda that captures
    # and saves the output.
    with patch.object(GetPanDaStat, "querypanda", new=capture_querypanda):
        get_panda_stat = GetPanDaStat(**get_panda_stat_kwargs)
        get_panda_stat.run()


def main():
    """Run the command line interface."""
    return cli()


if __name__ == "__main__":
    main()
