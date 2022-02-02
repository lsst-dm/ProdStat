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

from urllib.request import urlopen
import json
import gzip

import click

__all__ = ['main']

@click.group()
def cli():
    """Command line interface creating ProdStat test data files."""

@cli.command()
@click.option("--fname", default="wfprogress.json.gz", show_default=True, type=str,
              help="Output file name.")
@click.option("--url", default='http://panda-doma.cern.ch/idds/wfprogress/?json',
              show_default=True, type=str,
              help="Input URL.")
def get_wfprogress(fname, url='http://panda-doma.cern.ch/idds/wfprogress/?json'):
    with urlopen(url) as url_response:
        content = json.load(url_response)

    this_open = gzip.open if fname.endswith('.gz') else open
    with this_open(fname, 'wt', encoding='UTF-8') as out_io:
        json.dump(content, out_io, indent=4)
        

def main():
    """Run the command line interface."""
    return cli()

if __name__=="__main__":
    main()
    