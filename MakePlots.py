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
import sys
import os
import getopt
import yaml
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


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


if __name__ == "__main__":
    print(sys.argv)
    nb_par = len(sys.argv)
    if nb_par < 1:
        print("Usage: MakePlots.py <required inputs>")
        print("Required inputs:")
        print("-f <inputYaml> - yaml file with input parameters")
        sys.exit(-2)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:", ["inputYaml="])
    except getopt.GetoptError:
        print("Usage: MakePlots.py <required inputs>")
        print("Required inputs:")
        print("-f <inputYaml> - yaml file with input parameters")
        sys.exit(2)
    f_flag = 0
    inpF = ''
    for opt, arg in opts:
        print("%s %s" % (opt, arg))
        if opt == "-h":
            print("Usage: MakePlots.py <required inputs>")
            print("  Required inputs:")
            print("  -f <inputYaml> - yaml file with input parameters")
            print("The yaml file format as following:")
            print("bin_width: width of the plot bin in sec. \n",
                  "start_at: time of the plot start in hours \n",
                  "stop_at: time where plot should stop in hours \n")
            sys.exit(2)
        elif opt in ("-f", "--inputYaml"):
            f_flag = 1
            inpF = arg
    inpsum = f_flag
    if inpsum != 1:
        print("Usage: MakePlots.py <required inputs>")
        print("  Required inputs:")
        print("  -f <inputYaml> - yaml file with input parameters")
        sys.exit(-2)
    # Create new threads
    with open(inpF) as pf:
        inpars = yaml.safe_load(pf)
    MKP = MakePlots(**inpars)
    MKP.run()
    print("End with MakePlots.py")
