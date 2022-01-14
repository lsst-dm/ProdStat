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
from DRPUtils import DRPUtils


@click.group()
def cli():
    """Command line interface for ProdStat."""
    pass


@cli.command()
@click.argument("template", type=str)
@click.argument("band", type=click.Choice(["all", "f", "u", "g", "r", "i", "z", "y"]))
@click.argument("groupsize", type=int)
@click.argument("skipgroups", type=int)
@click.argument("ngroups", type=int)
@click.argument("explist", type=str)
def make_prod_groups(template, band, groupsize, skipgroups, ngroups, explist):
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
    DRPUtils.make_prod_groups(template, band, groupsize, skipgroups, ngroups, explist)


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
    butler_uri = inpars["Butler"]
    butler_stat_getter = GetButlerStat(**params)
    butler_stat_getter.set_butler(butler_uri)
    butler_stat_getter.run()


if __name__ == "__main__":
    cli()
