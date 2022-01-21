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
# coding: utf-8
"""Command line interface for ProdStat."""

import yaml

import click
from .. import DRPUtils
from .. import GetButlerStat


@click.group()
def cli():
    """Command line interface for ProdStat."""


@cli.command()
@click.argument("template", type=str)
@click.argument("band", type=click.Choice(["all", "f", "u", "g", "r", "i", "z", "y"]))
@click.argument("groupsize", type=int)
@click.argument("skipgroups", type=int)
@click.argument("ngroups", type=int)
@click.argument("explist", type=str)
def make_prod_groups(
    template, band, groupsize, skipgroups, ngroups, explist
):  # pylint: disable=too-many-arguments
    """Split a list of exposures into groups defined in yaml files.

    \b
    Parameters
    ----------
    template : `str`
        Template file with place holders for start/end dataset/visit/tracts
        (optional .yaml suffix here will be added)
    band : `str`
        Which band to restrict to (or 'all' for no restriction, matches BAND
        in template if not 'all')
    groupsize : `int`
        How many visits (later tracts) per group (i.e. 500)
    skipgroups: `int`
        skip <skipgroups> groups (if others generating similar campaigns)
    ngroups : `int`
        how many groups (maximum)
    explists : `str`
        text file listing <band1> <exposure1> for all visits to use
    """
    DRPUtils.DRPUtils.make_prod_groups(
        template, band, groupsize, skipgroups, ngroups, explist
    )


@cli.command()
@click.argument("param_file", type=click.File(mode="r"))
def get_butler_stat(param_file):
    """Get butler Statistics.

    \b
    Parameters
    ----------
    param_file : `typing.TextIO`
        A file from which to read butler parameters

    \b
    Note
    ----
    The yaml file follows this format::

        Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml
        Jira: PREOPS-707
        collType: 2.2i
        workNames: not used now
        maxtask: 100
    """
    params = yaml.safe_load(param_file)
    butler_uri = params["Butler"]
    butler_stat_getter = GetButlerStat.GetButlerStat(**params)
    butler_stat_getter.set_butler(butler_uri)
    butler_stat_getter.run()


@cli.command()
@click.argument("bps_submit_fname", type=str)
@click.argument("production_issue", type=str)
@click.argument("drp_issue", required=False, default="DRP0", type=str)
@click.option("--ts", default="0", type=str)
def update_issue(bps_submit_fname, production_issue, drp_issue, ts):
    """Update or create a DRP issue.

    \b
    Parameters
    ----------
    pbs_submit_fname : `str`
        The file name for the BPS submit file (yaml).
        Should be sitting in the same dir that bps submit was done,
        so that the submit/ dir can be searched for more info
    production_issue : `str`
        PREOPS-938 or similar production issue for this group of
        bps submissions
    drp_issue : `str`
        DRP issue created to track ProdStat for this bps submit
    ts : `str`
        unknown
    """
    drp = DRPUtils.DRPUtils()
    drp.drp_issue_update(bps_submit_fname, production_issue, drp_issue, ts)


@cli.command()
@click.argument("production_issue", type=str)
@click.argument("drp_issue", type=str)
@click.option("--reset", default=False, type=bool)
@click.option("--remove", default=False, type=bool)
def add_job_to_summary(production_issue, drp_issue, reset, remove):
    """Add a summary to a job summary table.

    \b
    Parameters
    ----------
    production_issue : `str`
        campaign defining ticket, also in the butler output name
    drp_issue : `str`
        the issue created to track ProdStat for this bps submit
    reset : `bool`
        erase the whole table (don't do this lightly)
    remove : `bool`
        remove one entry from the table with the DRP/PREOPS number
    """
    if reset and remove:
        print("Either reset or remove can be set, but not both.")

    if reset:
        first = 1
    elif remove:
        first = 2
    else:
        first = 0

    frontend = "DRP-53"
    frontend1 = "DRP-55"
    backend = "DRP-54"
    ts = "-1"
    status = "-1"
    drp = DRPUtils.DRPUtils()
    drp.drp_add_job_to_summary(
        first, ts, production_issue, drp_issue, status, frontend, frontend1, backend
    )


@cli.command()
@click.argument("production_issue", type=str)
@click.argument("drp_issue", required=False, default="DRP0", type=str)
def update_stat(production_issue, drp_issue):
    """Update issue statistics.

    \b
    Parameters
    ----------
    production_issue : `str`
        campaign defining ticket, also in the butler output name
    drp_issue : `str`
        leave off if you want a new issue generated, to redo,
        include the DRP-issue generated last time
    """
    drp_utils = DRPUtils.DRPUtils()
    drp_utils.drp_stat_update(production_issue, drp_issue)


def main():
    return cli()

