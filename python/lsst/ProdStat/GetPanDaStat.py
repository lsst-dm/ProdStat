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
import json
import re
import urllib.error as url_error
from urllib.request import urlopen
import time
from time import sleep, gmtime, strftime
import datetime
import math
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table

__all__ = ['GetPanDaStat']


class GetPanDaStat:
    """Build production statistics tables using PanDa database queries.

    Parameters
    ----------
    Jira : `str`
        Jira ticket identifying production campaign used
        to select campaign workflows
    CollType : `str`
        token that with jira ticket will uniquely define campaign workflows
    startTime : `str`
        time to start selecting workflows from in Y-m-d format
    stopTime : `str`
        time to stop selecting workflows in Y-m-d
    maxtask : `int`
        maximum number of task files to analyse
    """

    def __init__(self, **kwargs):

        self.collType = kwargs["collType"]
        self.Jira = kwargs["Jira"]
        self.start_date = kwargs["start_date"]
        self.stop_date = kwargs["stop_date"]
        self.maxtask = int(kwargs["maxtask"])
        self.workKeys = list()
        print(" Collecting information for Jira ticket ", self.Jira)
        self.workflows = dict()
        self.wfInfo = dict()  # workflow status
        self.taskCounts = dict()  # number of tasks of given type
        self.allTasks = dict()  # info about tasks
        self.allJobs = dict()  # info about jobs
        self.wfTasks = dict()  # tasks per workflow
        self.taskStat = dict()
        self.allStat = dict()  # general statistics
        self.wfNames = dict()
        self.start_stamp = datetime.datetime.strptime(self.start_date, "%Y-%m-%d").timestamp()
        self.stop_stamp = datetime.datetime.strptime(self.stop_date, "%Y-%m-%d").timestamp()

    def get_workflows(self):
        """First lets get all workflows with given keys."""

        wfdata = self.querypanda(
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
                print('key=', key, ' date_str=', date_str, ' date_stamp=', date_stamp)
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

    def getwftasks(self, workflow):
        """Select tasks for given workflow (jobs).

        Parameters
        ----------
        workflow : `str`
            workflow name

        Returns
        -------
        tasks : `list`
            list of tasks in given workflow
        """

        urls = workflow["r_name"]
        tasks = self.querypanda(
            urlst="http://panda-doma.cern.ch/tasks/?taskname="
                  + urls
                  + "*&days=120&json"
        )
        return tasks

    def gettaskinfo(self, task):
        """Extract data we need from task dictionary.

        Parameters
        ----------
        task : `dict`
            dictionary of task parameters

        Returns
        -------
        data : `dict`
            subset of the task data
        """

        data = dict()
        jeditaskid = task["jeditaskid"]

        # Now select a number of jobs to calculate average cpu time and max Rss
        uri = (
            "http://panda-doma.cern.ch/jobs/?jeditaskid="
            + str(jeditaskid)
            + "&limit="
            + str(self.maxtask)
            + "&jobstatus=finished&json"
        )
        jobsdata = self.querypanda(urlst=uri)
        """ list of jobs in the task """
        jobs = jobsdata["jobs"]
        njobs = len(jobs)
        corecount = 0
        max_rss = 0
        duration = 0
        attempts = 0
        starttime = float(round(time.time()))
        if njobs > 0:
            for jb in jobs:
                corecount += jb["actualcorecount"]
                duration += jb["durationsec"]
                attempts += jb["attemptnr"]
                tokens = jb["starttime"].split("T")
                startst = tokens[0] + " " + tokens[1]  # get rid of T in the date string
                taskstart = datetime.datetime.strptime(
                    startst, "%Y-%m-%d %H:%M:%S"
                ).timestamp()
                if starttime >= taskstart:
                    starttime = taskstart
                if max_rss <= jb["minramcount"]:
                    max_rss = jb["minramcount"]
            corecount = float(corecount / njobs)
            duration = float(duration / njobs)
            attemptnr = float(attempts / njobs)
        else:
            return data
        """select first good job """
        for jb in jobs:
            if jb["jobstatus"] != "failed":
                break
        """Fill data with the firs good job """
        dsinfo = task["dsinfo"]
        data["jeditaskid"] = task["jeditaskid"]
        data["jobname"] = jb["jobname"]
        data["taskname"] = task["taskname"]
        data["status"] = task["status"]
        data["attemptnr"] = int(attemptnr)
        data["actualcorecount"] = jb["actualcorecount"]
        data["starttime"] = str(task["starttime"]).upper()
        if task["endtime"] is None:
            _now = datetime.datetime.now()
            task["endtime"] = _now.strftime("%Y-%m-%dT%H:%M:%S")
        _endtime = str(task["endtime"]).upper()
        tokens = _endtime.split("T")
        data["endtime"] = tokens[0] + " " + tokens[1]  # get rid of T in the date string
        if task["starttime"] is None:
            task["starttime"] = tokens[0] + " " + tokens[1]
        data["starttime"] = task["starttime"]
        data["maxattempt"] = jb["maxattempt"]
        data["basewalltime"] = task["basewalltime"]
        data["cpuefficiency"] = task["cpuefficiency"]
        data["maxdiskcount"] = jb["maxdiskcount"]
        data["maxdiskunit"] = jb["maxdiskunit"]
        data["cpuconsumptiontime"] = duration
        data["jobstatus"] = jb["jobstatus"]
        tokens = jb["starttime"].split("T")
        data["jobstarttime"] = (
            tokens[0] + " " + tokens[1]
        )  # get rid of T in the date string
        tokens = jb["endtime"].split("T")
        data["jobendtime"] = (
            tokens[0] + " " + tokens[1]
        )  # get rid of T in the date string
        taskstart = datetime.datetime.strptime(
            data["starttime"], "%Y-%m-%d %H:%M:%S"
        ).timestamp()

        jobstart = starttime
        taskend = datetime.datetime.strptime(
            data["endtime"], "%Y-%m-%d %H:%M:%S"
        ).timestamp()
        jobduration = taskend - jobstart
        data["ncpus"] = corecount
        data["taskduration"] = jobduration
        data["exeerrorcode"] = jb["exeerrorcode"]
        data["nfiles"] = dsinfo["nfiles"]
        data["Rss"] = max_rss
        return data

    def gettaskdata(self, key, tasks):
        """Given list of jobs get statistics for each job type.

        Parameters
        ----------
        key : `str`
            timestamp part of the workflow name
        tasks : `list`
            list of tasks data dictionaries

        Returns
        -------
        tasktypes : `dict`
            dictionary of task types with list of tasks
        """

        taskdata = list()
        tasknames = list()
        tasktypes = dict()
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
            comp = key.upper()
            taskname = task["taskname"].split(comp)[1]
            tokens = taskname.split("_")
            name = ""
            for i in range(1, len(tokens) - 1):
                name += tokens[i] + "_"
            taskname = name[:-1]
            data = self.gettaskinfo(task)
            if len(data) == 0:
                print("No data for ", taskname)
                continue
            jobname = data["jobname"].split("Task")[0]
            taskname = data["taskname"]
            comp = key.upper()
            taskname = taskname.split(comp)[1]
            tokens = taskname.split("_")
            name = ""
            for i in range(1, len(tokens) - 1):
                name += tokens[i] + "_"
            taskname = name[:-1]
            taskname = str(key) + "_" + taskname
            data["taskname"] = taskname
            data["jobname"] = jobname
            if jobname not in self.allJobs:
                self.allJobs[jobname] = []
            if taskname not in self.allJobs[jobname]:
                self.allJobs[jobname].append(taskname)
            data["walltime"] = data["taskduration"]
            taskdata.append(data)
        """Now create a list of task types"""
        for data in taskdata:
            name = data["taskname"]
            if name not in self.taskCounts:
                self.taskCounts[name] = 0
                self.allTasks[name] = list()
                tasknames.append(name)
        """Now create a list of tasks for each task type """
        for taskname in tasknames:
            tasklist = list()
            for task in taskdata:
                if taskname == task["taskname"]:
                    tasklist.append(task)
                    if taskname in self.allTasks:
                        self.allTasks[taskname].append(task)
                        self.taskCounts[taskname] += 1
            tasktypes[taskname] = tasklist
        return tasktypes

    def gettasks(self):
        """Select finished and sub finished workflow tasks."""

        for key in self.workKeys:
            self.wfTasks[key] = list()
            _workflows = self.workflows[key]
            for wf in _workflows:
                if (
                        str(wf["r_status"]) == "finished"
                        or str(wf["r_status"]) == "subfinished"
                        or str(wf["r_status"]) == "running"
                        or str(wf["r_status"]) == "transforming"
                ):
                    """get tasks for this workflow"""
                    tasks = self.getwftasks(wf)
                    """get data for each task """
                    tasktypes = self.gettaskdata(key, tasks)
                    self.wfTasks[key].append(tasktypes)

    @staticmethod
    def querypanda(urlst):
        """Read given URL to get panda data

        Parameters
        ----------
        urlst : `str`
            URL string to get data from

        Returns
        -------
        result : `dict`
            dictionary of panda data from given URL
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

    def getallstat(self):
        """Calculate campaign statistics."""

        wfwall = 0
        wfdisk = 0
        wfcores = 0
        wf_rss = 0
        wfduration = 0.0
        wfnfiles = 0
        wfparallel = 0
        self.allStat = dict()
        for ttype in self.allTasks:
            self.allStat[ttype] = dict()
            tasks = self.allTasks[ttype]
            ntasks = len(tasks)
            cpuconsumption = 0
            walltime = 0
            cpuefficiency = 0
            corecount = 0
            maxdiskcount = 0
            duration = 0.0
            taskduration = 0.0
            nfiles = 0
            max_rss = 0
            attempts = 0.0
            ntasks = len(tasks)
            for i in range(ntasks):
                walltime += int(tasks[i]["walltime"])
                cpuconsumption += int(tasks[i]["cpuconsumptiontime"])
                cpuefficiency += int(tasks[i]["cpuefficiency"])
                maxdiskcount += int(tasks[i]["maxdiskcount"])
                duration += float(tasks[i]["cpuconsumptiontime"])
                attempts += tasks[i]["attemptnr"]
                starttime = tasks[i]["starttime"]
                taskduration += tasks[i]["taskduration"]
                corecount += int(tasks[i]["actualcorecount"])
                nfiles = int(tasks[i]["nfiles"])
                rss = tasks[i]["Rss"]
                if max_rss <= rss:
                    max_rss = rss
            taskduration = taskduration / ntasks
            walltime_pj = cpuconsumption / ntasks
            walltime = walltime_pj * nfiles
            maxdisk_pj = maxdiskcount / ntasks
            diskcount = maxdisk_pj * nfiles
            corecount_pj = corecount / ntasks
            corecount = corecount_pj * nfiles
            if taskduration <= 0.:
                nparallel = 1
            else:
                nparallel = int(math.ceil(walltime / taskduration))
            if nparallel < 1:
                nparallel = 1
            wfduration += taskduration
            wfwall += walltime
            wfdisk += diskcount
            wfcores += nparallel
            wfnfiles += nfiles
            if wf_rss <= max_rss:
                wf_rss = max_rss
            self.allStat[ttype] = {
                "nQuanta": float(nfiles),
                "starttime": starttime,
                "wallclock": str(datetime.timedelta(seconds=taskduration)),
                "cpu sec/job": float(walltime_pj),
                "cpu-hours": str(datetime.timedelta(seconds=walltime)),
                "est. parallel jobs": nparallel,
            }

        if wfduration > 0:
            wfparallel = int(math.ceil(wfwall / wfduration))
        else:
            wfparallel = 0
        self.allStat["Campaign"] = {
            "nQuanta": float(wfnfiles),
            "starttime": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            "wallclock": str(datetime.timedelta(seconds=wfduration)),
            "cpu sec/job": "-",
            "cpu-hours": str(datetime.timedelta(seconds=wfwall)),
            "est. parallel jobs": wfparallel,
        }

    @staticmethod
    def highlight_status(value):
        """Create background color for HTML table.

        Parameters
        ----------
        value : `str`
            status of the job

        Returns
        -------
        backgroupd_colors : `list`
            background color
        """
        if str(value) == "failed":
            return ["background-color: read"] * 9
        elif str(value) == "subfinisher":
            return ["background-color: yellow"] * 9
        else:
            return ["background-color: green"] * 9

    @staticmethod
    def highlight_greaterthan_0(s):
        """Create background color for HTML table

        Parameters
        ----------
        s : `int`
            task status flag

        Returns
        -------
        background_colors : `list`
            background color
        """
        if s.task_failed > 0.0:
            return ["background-color: red"] * 9
        elif s.task_subfinished > 0.0:
            return ["background-color: yellow"] * 9
        else:
            return ["background-color: white"] * 9

    def make_table_from_csv(self, buffer, out_file, index_name, comment):
        """Create Jira table from csv file.

        Parameters
        ----------
        buffer : `str`
            comma separated data buffer
        out_file : `str`
            output file name
        index_name : `list`
            list of index names to name table rows
        comment : `str`
            additional string to be added at top of the table
        """
        newbody = comment + "\n"
        newbody += out_file + "\n"
        lines = buffer.split("\n")
        comma_matcher = re.compile(r",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)")
        i = 0
        for line in lines:
            if i == 0:
                tokens = line.split(",")
                line = "|" + index_name
                for ln in range(1, len(tokens)):
                    line += "||" + tokens[ln]
                line += "||\r\n"
            elif i >= 1:
                tokens = comma_matcher.split(line)
                line = "|"
                for token in tokens:
                    line += token + "|"
                line = line[:-1]
                line += "|\r\n"
            newbody += line
            i += 1
        new_body = newbody[:-2]
        with open("/tmp/" + out_file + "-" + self.Jira + ".txt", "w") as tb_file:
            print(new_body, file=tb_file)
        return newbody

    def make_styled_table(self, dataframe, outfile):
        """Create styled HTML table.

        Parameters
        ----------
        dataframe : `pandas.DataFrame`
            pandas data frame containing table data
        outfile : `str`
            output file name
        :return:
        """
        df_styled = dataframe.style.apply(self.highlight_greaterthan_0, axis=1)
        df_styled.set_table_attributes('border="1"')
        df_html = df_styled.render()
        htfile = open("/tmp/" + outfile + "-" + self.Jira + ".html", "w")
        print(df_html, file=htfile)
        htfile.close()

    def make_table(self, data_frame, table_name, index_name, comment):
        """Create several types of tables from pandas data frame

        Parameters
        ----------
        data_frame : `pandas.DataFrame`
            pandas data frame
        table_name : `str`
            name of the output table
        index_name : `list`
            list of raw names
        comment : `str`
            additional text information to put at top of the table
        """
        fig, ax = plt.subplots(figsize=(20, 35))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabula = table(ax, data_frame, loc="upper right")
        tabula.auto_set_font_size(False)  # Activate set fontsize manually
        tabula.auto_set_column_width(col=list(range(len(data_frame.columns))))
        tabula.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabula.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/" + table_name + "-" + self.Jira + ".png", transparent=True)
        plt.show()
        html_buff = data_frame.to_html(index=True)
        html_file = open("/tmp/" + table_name + "-" + self.Jira + ".html", "w")
        html_file.write(html_buff)
        html_file.close()
        data_frame.to_csv("/tmp/" + table_name + "-" + self.Jira + ".csv", index=True)
        csbuf = data_frame.to_csv(index=True)
        self.make_table_from_csv(csbuf, table_name, index_name, comment)

    def run(self):
        """Run the program."""
        self.get_workflows()
        self.gettasks()
        self.getallstat()
        print("workflow info")
        wfind = list()
        wflist = list()
        #        wfIndF = open('./wfInd.txt','w')
        """ Let sort datasets by creation time"""
        _dfids = dict()
        _dfkeys = list()
        for key in self.wfInfo:
            """wfInd.append(self.wfNames[key])"""
            utime = self.wfInfo[key]["created"]
            _sttime = datetime.datetime.utcfromtimestamp(utime)
            self.wfInfo[key]["created"] = _sttime
            _dfids[key] = utime
        #
        for key in dict(sorted(_dfids.items(), key=lambda item: item[1])):
            wfind.append(str(key))
            _dfkeys.append(key)
            wflist.append(self.wfInfo[key])

        pd.set_option("max_colwidth", 500)
        pd.set_option("precision", 1)
        dataframe = pd.DataFrame(wflist, index=wfind)
        comment = " workflow status " + self.Jira
        index_name = "workflow"
        table_name = "pandaWfStat"
        self.make_table(dataframe, table_name, index_name, comment)
        self.make_styled_table(dataframe, table_name)
        _taskids = dict()
        ttypes = list()
        statlist = list()
        """Let's sort entries by start time"""
        for ttype in self.allStat:
            utime = self.allStat[ttype]["starttime"]
            utime = datetime.datetime.strptime(utime, "%Y-%m-%d %H:%M:%S").timestamp()
            _taskids[ttype] = utime
        #
        for ttype in dict(sorted(_taskids.items(), key=lambda item: item[1])):
            ttypes.append(ttype)
            statlist.append(self.allStat[ttype])
        dfs = pd.DataFrame(statlist, index=ttypes)
        table_name = "pandaStat"
        index_name = " Workflow Task "
        comment = " Panda campaign statistics " + self.Jira
        self.make_table(dfs, table_name, index_name, comment)
