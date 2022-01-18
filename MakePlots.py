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
import os
import yaml
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import click


class MakePlots:
    def __init__(self, **kwargs):
        """
        Class to build production jobs time distribution using csv time series
        stored in /tmp and produced by MakePandaPlots.py
        """
        self.bin_width = float(kwargs['bin_width'])
        " bin width in hours "
        self.scale_factor = self.bin_width / 3600.
        self.stop_at = int(kwargs['stop_at'])
        self.start_at = float(kwargs['start_at'])
        " bin_width in seconds, start_at and stop_at in hours "
        self.plot_n_bins = int((self.stop_at - self.start_at) / self.scale_factor)
        " Add more job names for other production steps "
        self.job_names = ['pipetaskInit', 'visit_focal_plane', 'mergeExecutionButler']

    def make_plot(self, data_list, max_time, job_name):
        first_bin = int(self.start_at / self.scale_factor)
        last_bin = first_bin + self.plot_n_bins
        n_bins = int(max_time / self.bin_width)
        task_count = np.zeros(n_bins)
        for time_in, duration in data_list:
            task_count[int(time_in / self.bin_width):int((time_in + duration) / self.bin_width)] += 1
        if self.plot_n_bins > n_bins:
            last_bin = n_bins
        sub_task_count = np.copy(task_count[first_bin:last_bin])
        max_y = 1.1 * (max(sub_task_count) + 1.)
        sub_task_count.resize(self.plot_n_bins)
        x_bins = np.arange(self.plot_n_bins) * self.scale_factor + self.start_at
        plt.plot(x_bins, sub_task_count, label=str(job_name))
        plt.axis([self.start_at, self.stop_at, 0, max_y])
        plt.xlabel("Hours since first quantum start")
        plt.ylabel("Number of running quanta")
        plt.savefig("timing_" + job_name + ".png")

    def run(self):
        for job_name in self.job_names:
            data_file = "/tmp/" + 'panda_time_series_' + job_name + ".csv"
            if os.path.exists(data_file):
                df = pd.read_csv(data_file, header=0, index_col=0,
                                 parse_dates=True, squeeze=True)
                data_list = list()
                max_time = 0.
                for index, row in df.iterrows():
                    if float(row[0]) >= max_time:
                        max_time = row[0]
                    data_list.append((row[0], row[1]))
                print(" job name ", job_name)
                self.make_plot(data_list, max_time, job_name)


@click.group()
def cli():
    """Command line interface for ProdStat."""
    pass


@cli.command()
@click.argument("param_file", type=click.File(mode="r"))
def make_plots(param_file):
    """Create  timing plots of the campaign jobs using timing
    information produced by MakePandaPlots.py
    Parameters
    ----------
    param_file : `typing.TextIO`
        A file from which to read  parameters
    Note
    ----
    The yaml file should provide following parameters::
        bin_width: 30. bin width in seconds
        start_at: 0. start plot time  in hours
        stop_at: 550. end plot time in hours
    """
    click.echo('Start with MakePlots')
    params = yaml.safe_load(param_file)
    plot_maker = MakePlots(**params)
    plot_maker.run()
    print("Finish with MakePlots")


cli.add_command(make_plots)

if __name__ == "__main__":
    cli()
