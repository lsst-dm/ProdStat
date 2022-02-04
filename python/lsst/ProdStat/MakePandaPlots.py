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
import json
import urllib.error as url_error
from urllib.request import urlopen
from time import sleep
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


__all__ = ['MakePandaPlots']


class MakePandaPlots:
    """Build production statistics tables using PanDa database queries.

    Parameters
    ----------
    Jira : `str`
        Jira ticket identifying production campaign used
        to select campaign workflows
    collType : `str`
        token that with jira ticket will uniquely define the dataset (workflow)
    bin_width : `str`
        plot bin width in sec.
    start_at : `float`
        time in hours at which to start plot
    stop_at : `float`
        time in hours at which to stop plot
    startTime : `str`
        time to start selecting workflows from in Y-m-d format
    stopTime : `str`
        time to stop selecting workflows in Y-m-d format
    """

    def __init__(self, **kwargs):
        self.collType = kwargs["collType"]
        self.Jira = kwargs["Jira"]
        " bin width in seconds "
        self.bin_width = kwargs["bin_width"]
        " bin width in hours "
        self.scale_factor = float(self.bin_width) / 3600.0
        self.stop_at = int(kwargs["stop_at"])
        self.start_at = float(kwargs["start_at"])
        self.start_date = kwargs["start_date"]
        self.stop_date = kwargs["stop_date"]
        self.plot_n_bins = int((self.stop_at - self.start_at)
                               / self.scale_factor)
        self.start_time = 0
        self.workKeys = list()
        print(" Collecting information for Jira ticket ", self.Jira)
        self.workflows = dict()
        self.wfInfo = dict()  # workflow status
        self.taskCounts = dict()  # number of tasks of given type
        self.allTasks = dict()  # info about tasks
        self.allJobs = dict()  # info about jobs
        self.wfTasks = dict()  # tasks per workflow
        self.job_names = kwargs["job_names"]
        self.wfNames = dict()
        self.start_stamp = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").timestamp()
        self.stop_stamp = datetime.datetime.strptime(self.stop_date, "%Y-%m-%d").timestamp()

    def get_workflows(self):
        """First lets get all workflows with given keys.
        """

        wfdata = self.query_panda(
            urlst="http://panda-doma.cern.ch/idds/wfprogress/?json"
        )
        comp = str(self.Jira).lower()
        comp1 = str(self.collType)
        nwf = 0
        for wf in wfdata:
            r_name = wf["r_name"]
            if comp in r_name and comp1 in r_name:
                key = str(r_name).split("_")[-1]
                date_str = key.split('t')[0]
                date_stamp = datetime.datetime.strptime(date_str, "%Y%m%d").timestamp()
                if self.start_stamp <= date_stamp <= self.stop_stamp:
                    self.workKeys.append(str(key))
                    nwf += 1
        print("number of workflows =", nwf)
        if nwf == 0:
            print("No workflows to work with -- exiting")
            sys.exit(-256)
        for key in self.workKeys:
            self.workflows[key] = []
        for wfk in self.workKeys:
            for wf in wfdata:
                r_name = wf["r_name"]
                if wfk in r_name:
                    self.workflows[wfk].append(wf)
        #
        print("Selected workflows:", self.workflows)
        #        print(self.wfNames)
        create_time = list()
        for key in self.workKeys:
            workflow = self.workflows[key]
            for wf in workflow:
                created = datetime.datetime.strptime(
                    wf["created_at"], "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                r_status = wf["r_status"]
                total_tasks = wf["total_tasks"]
                total_files = wf["total_files"]
                remaining_files = wf["remaining_files"]
                processed_files = wf["processed_files"]
                task_statuses = wf["tasks_statuses"]
                create_time.append(created)
                print(
                    "created",
                    created,
                    " total tasks ",
                    total_tasks,
                    " total files ",
                    total_files,
                )
                if "Finished" in task_statuses.keys():
                    finished = task_statuses["Finished"]
                else:
                    finished = 0
                if "SubFinished" in task_statuses.keys():
                    subfinished = task_statuses["SubFinished"]
                else:
                    subfinished = 0
                if "Failed" in task_statuses.keys():
                    failed = task_statuses["Failed"]
                else:
                    failed = 0
                if key not in self.wfInfo:
                    self.wfInfo[key] = {
                        "status": r_status,
                        "ntasks": float(total_tasks),
                        "nfiles": float(total_files),
                        "remaining files": float(remaining_files),
                        "processed files": float(processed_files),
                        "task_finished": float(finished),
                        "task_failed": float(failed),
                        "task_subfinished": float(subfinished),
                        "created": created,
                    }
        self.start_time = min(create_time)
        print("all started at ", self.start_time)

    def get_wf_tasks(self, workflow):
        """Select tasks for given workflow (jobs).

        Parameters
        ----------
        workflow: `str`
            workflow for which tasks will be selected

        Returns
        -------
        tasks: `list`
            list of tasks in given workflow
        """
        urls = str(workflow["r_name"])
        tasks = self.query_panda(
            urlst="http://panda-doma.cern.ch/tasks/?taskname="
                  + urls
                  + "*&days=120&json"
        )
        return tasks

    def get_task_info(self, task):
        """Extract data we need from task dictionary.

        Parameters
        ----------
        task : `dic`
            dictionary with task information
        """

        jeditaskid = task["jeditaskid"]
        """ Now select jobs to get timing information """
        uri = "http://panda-doma.cern.ch/jobs/?jeditaskid=" + \
              str(jeditaskid) + "&json"
        jobsdata = self.query_panda(urlst=uri)
        """ list of jobs in the task """
        jobs = jobsdata["jobs"]
        n_jobs = len(jobs)
        if n_jobs > 0:
            for jb in jobs:
                job_name = jb["jobname"]
                if isinstance(jb["durationsec"], type(None)):
                    durationsec = 0.0
                else:
                    durationsec = float(jb["durationsec"])
                if isinstance(jb["starttime"], str):
                    tokens = jb["starttime"].split("T")
                    startst = (
                        tokens[0] + " " + tokens[1]
                    )  # get rid of T in the date string
                    taskstart = datetime.datetime.strptime(
                        startst, "%Y-%m-%d %H:%M:%S"
                    ).timestamp()
                    delta_time = taskstart - self.start_time
                else:
                    delta_time = -1.0
                for _name in self.job_names:
                    if _name in job_name:
                        if _name in self.allJobs:
                            self.allJobs[_name].append((delta_time,
                                                        durationsec))
                        else:
                            self.allJobs[_name] = list()
                            self.allJobs[_name].append((delta_time,
                                                        durationsec))
        #            print(self.allJobs)
        else:
            return
        return

    def get_task_data(self, tasks):
        """Given list of jobs get statistics for each job type.

        Parameters
        ----------
        tasks : `list`
            list of tasks
        """
        taskids = dict()
        """Let's sort tasks with jeditaskid """
        i = 0
        for task in tasks:
            _id = task["jeditaskid"]
            taskids[_id] = i
            i += 1
        for _id in sorted(taskids):
            tind = taskids[_id]
            task = tasks[tind]
            #            comp = key.upper()
            self.get_task_info(task)
        return

    def get_tasks(self):
        """Select all workflow tasks.
        """
        for key in self.workKeys:
            self.wfTasks[key] = list()
            _workflows = self.workflows[key]
            for wf in _workflows:
                """get tasks for this workflow"""
                tasks = self.get_wf_tasks(wf)
                """get data for each task """
                self.get_task_data(tasks)

    @staticmethod
    def query_panda(urlst):
        """Read url with panda information.

        Parameters
        ----------
        urlst : `str`
            URL string to read

        Returns
        -------
        result : `dict`
            dictionary with panda data
        """
        success = False
        ntryes = 0
        result = dict()
        while (not success) or (ntryes >= 5):
            try:
                with urlopen(urlst) as url:
                    result = json.loads(url.read().decode())
                    success = True
            except url_error.URLError:
                print("failed with ", urlst, " retrying")
                print("ntryes=", ntryes)
                success = False
                ntryes += 1
                sleep(2)
        sys.stdout.write(".")
        sys.stdout.flush()
        return result

    def make_plot(self, data_list, max_time, job_name, n_fig):
        """Plot timing data in png file.

        Parameters
        ----------
        data_list : `list`
            list of tuples (start_time, duration) for given job
        max_time : `float`
            maximal time in the timing data list
        job_name : `str`
            name of the job to be used in the name of plot file
        n_fig : 'int' figure number
        """
        first_bin = int(self.start_at / self.scale_factor)
        last_bin = first_bin + self.plot_n_bins
        n_bins = int(max_time / self.bin_width)
        task_count = np.zeros(n_bins)
        for time_in, duration in data_list:
            task_count[int(time_in / self.bin_width): int(
                (time_in + duration) / self.bin_width)] += 1
        if self.plot_n_bins > n_bins:
            last_bin = n_bins
        sub_task_count = np.copy(task_count[first_bin:last_bin])
        max_y = 1.2 * (max(sub_task_count) + 1.0)
        sub_task_count.resize([self.plot_n_bins])
        x_bins = np.arange(self.plot_n_bins) * self.scale_factor + self.start_at
        plt.figure(n_fig)
        plt.plot(x_bins, sub_task_count, label=str(job_name))
        plt.axis([self.start_at, self.stop_at, 0, max_y])
        plt.xlabel("Hours since first quantum start")
        plt.ylabel("Number of running quanta")
        plt.savefig("timing_" + job_name + ".png")

    def prep_data(self):
        """Create file with timing data."""

        self.get_workflows()
        self.get_tasks()
        print(" all time data")
        for key in self.allJobs:
            self.allJobs[key].sort()
            print(key, ' ', self.allJobs[key])
        for job_name in self.allJobs.keys():
            dataframe = pd.DataFrame(
                self.allJobs[job_name], columns=["delta_time", "durationsec"]
            )
            dataframe.to_csv(
                "/tmp/" + "panda_time_series_" + job_name + ".csv", index=True
            )

    def plot_data(self):
        """Create plot of timing data in form of png file."""
        n_fig = 0
        for job_name in self.job_names:
            data_file = "/tmp/" + "panda_time_series_" + job_name + ".csv"
            if os.path.exists(data_file):
                df = pd.read_csv(
                    data_file, header=0, index_col=0, parse_dates=True,
                    squeeze=True)
                data_list = list()
                max_time = 0.0
                for index, row in df.iterrows():
                    if float(row[0]) >= max_time:
                        max_time = row[0]
                    data_list.append((row[0], row[1]))
                print(" job name ", job_name)
                self.make_plot(data_list, max_time, job_name, n_fig)
                n_fig += 1
