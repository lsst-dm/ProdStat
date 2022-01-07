#!/usr/bin/env python
"""Split a list of exposures into groups defined in yaml files.

Creates yaml files suitable for passing to `bps submit` that define
subsets of a provided list of exposures.
"""

import sys
import os
import pandas as pd
import numpy as np
import click


@click.command()
@click.argument(
    "template",
    "Template file with place holders for start/end dataset/visit/tracts (optional .yaml suffix here will be added)",
)
@click.argument(
    "band",
    "Which band to restrict to (or 'all' for no restriction, matches BAND in template if not 'all')",
)
@click.argument("groupsize", "How many visits (later tracts) per group (i.e. 500)")
@click.argument(
    "skipgroups",
    "skip <skipgroups> groups (if others generating similar campaigns)",
    type=int,
)
@click.argument("ngroups", "how many groups (maximum)", type=int)
@click.argument(
    "explist", "text file listing <band1> <exposure1> for all visits to use"
)
def make_prod_groups(template, band, groupsize, skipgroups, ngroups, explist):
    """Split a list of exposures into groups defined in yaml files.

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
    template_base = os.path.basename(template)
    template_fname, template_ext = os.path.splitext(template_base)
    out_base = template_fname if template_ext == ".yaml" else template_base

    with open(template, "r") as template_file:
        template_content = template_file.read()

    exposures = pd.read_csv(explist, names=["band", "exp_id"], delimiter="\s+")
    exposures.sort_values("exp_id", inplace=True)
    if band not in ("all", "f"):
        exposures.query("band=={band}", inplace=True)

    # Add a new column to the DataFrame with group ids
    num_exposures = len(exposures)
    exposures["group_id"] = np.floor(np.arange(num_exposures) / groupsize).astype(int)

    for group_id in range(skipgroups, skipgroups + ngroups):
        group_exposures = exposures.query(f"group_id == {group_id}")
        min_exp_id = group_exposures.exp_id.min()
        max_exp_id = group_exposures.exp_id.max()

        # Add 1 to the group id so it starts at 1, not 0
        group_num = group_id + 1
        out_content = (
            template_content.replace("GNUM", str(group_num))
            .replace("BAND", band)
            .replace("LOWEXP", str(min_exp_id))
            .replace("HIGHEXP", str(max_exp_id))
        )

        out_fname = f"{out_base}-{band}-{group_num}.yaml"
        with open(out_fname, "w") as out_file:
            out_file.write(out_content)


if __name__ == "__main__":
    make_prod_groups()
