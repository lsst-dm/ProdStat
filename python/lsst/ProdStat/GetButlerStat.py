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
import sys
import re
import os
import datetime
from time import gmtime, strftime
from collections import defaultdict
import yaml
import getopt
from tabulate import tabulate
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
from lsst.daf.butler import Butler
from lsst.daf.butler import ButlerURI
from lsst.daf.base import PropertySet


class GetButlerStat:
    """Build production statistics table using Butler meta data.

    Parameters
    ----------
    Butler : `str`
        TODO
    Jira : `str`
        TODO
    CollType : `str`
        token that with jira ticket will uniquely define the dataset (workflow)
    workNames : `str`
        Not used
    maxtask : `str`
        maximum number of task files to analyse
    """
        
    def __init__(self, **kwargs):

        if "Butler" in kwargs:
            self.Butler = kwargs["Butler"]
        else:
            self.Butler = ""
        self.collType = kwargs["collType"]
        self.workNames = kwargs["workNames"]
        self.Jira = kwargs["Jira"]
        self.maxtask = int(kwargs["maxtask"])
        print(" Collecting information for Jira ticket ", self.Jira)
        self.REPO_ROOT = self.Butler
        self.butler = Butler(self.REPO_ROOT)
        self.registry = self.butler.registry
        self.workflowRes = {}
        self.CollKeys = dict()
        self.collSize = dict()
        self.collData = dict()

    @staticmethod
    def parse_metadata_yaml(yaml_file):
        """Parse the runtime and RSS data in the metadata yaml.
        
        Parameters
        ----------
        yaml_file : `str`
            File name for the runtime yaml metadata file.
        
        Returns
        -------
        results : TODO
            TODO
        
        Note
        ----
        The yaml file should be file created by the lsst.pipe.base.timeMethod decorator
        as applied to pipetask methods.
        """
        
        time_types = "Cpu User System".split()
        min_fields = [f"Start{_}Time" for _ in time_types] + [
            f"start{_}Time" for _ in time_types
        ]
        max_fields = (
                [f"End{_}Time" for _ in time_types]
                + [f"end{_}Time" for _ in time_types]
                + ["MaxResidentSetSize"]
        )
        time_stamp = ["startUtc", "prepUtc"]
        results = dict()
        with open(yaml_file) as fd:
            md = yaml.safe_load(fd)
        methods = list(md.keys())
        for method in methods:
            for key, value in md[method].items():
                if key in time_stamp:
                    starts = value
                    if "T" in value:
                        tokens = starts.split("T")
                        startst = (
                                tokens[0] + " " + tokens[1]
                        )  # get rid of T in the date string
                    if "timestamp" not in results:
                        results["timestamp"] = startst
                for min_field in min_fields:
                    if min_field not in key:
                        continue
                    if min_field not in results or value < results[min_field]:
                        results[min_field] = float(value)
                        continue
                for max_field in max_fields:
                    if max_field not in key:
                        continue
                    if max_field not in results or value > results[max_field]:
                        results[max_field] = float(value)
                        continue
        return results

    def set_butler(self, butler_string):
        """Set the butler.
        
        Parameters
        ----------
        butler_string : `str`
            Defines how to access the butlery
        """
        
        self.Butler = butler_string

    def search_collections(self):
        """Select collections.
        
        Returns
        -------
        collections : `list`
            A list of collections.
        """
        
        collections = []
        preops = self.Jira
        for c in sorted(self.registry.queryCollections()):
            if preops in str(c) and self.collType in str(c):
                key = str(c).split(preops)[1]
                if len(key) > 1:
                    key = key.strip("/")
                    collections.append(c)
                    self.CollKeys[c] = key
        """ Now we have all collections with PREOPS in the name and collectionType
           like PREOPS-707 and 2.2i/runs/  in the collection name """
        print("selected collections ")
        print(collections)
        return collections

    def make_sum(self, task_size, task_res):
        """Calculate max RSS.
        
        Parameters
        ----------
        task_size : TODO
            TODO
        task_res : TODO
            TODO
            
        Returns
        -------
        summary : `dict`
            summary dictionary including:
            
            ``"nQuanta"``
                Number of quanta (`int`)
            ``"startTime"``
                TODO (TODO)
            ``"cpu sec/job"``
                TODO (`float`)
            ``"cpu-hours"``
                TODO (`float`)
            ``"MaxRSS GB"``
                TODO (`float`)
        """
        
        cputime = task_res["cpu_time"]
        max_rss = task_res["maxRSS"]
        time_start = task_res["startTime"]
        ts = min(int(task_size), self.maxtask)
        if cputime[0] is not None:
            cpu_sum = 0.0
            for t in cputime:
                cpu_sum += float(t)
            cpu_per_task = float(cpu_sum / ts)
            total_cpu = float(cpu_per_task * int(task_size))
        else:
            cpu_per_task = 0.0
            total_cpu = 0.0
        max_s = 0.0
        for s in max_rss:
            if float(s) >= max_s:
                max_s = float(s)
        return {
            "nQuanta": int(task_size),
            "startTime": time_start[0],
            "cpu sec/job": float(cpu_per_task),
            "cpu-hours": float(total_cpu),
            "MaxRSS GB": float(max_s / 1048576.0),
        }

    def gettaskdata(self, collections):
        """Collect datasets & IDs for each collection in subsets of IDs by process type.
        
        Parameters
        ----------
        collections : TODO
            TODO
        """
        
        datatype_pattern = ".*_metadata"
        pattern = re.compile(datatype_pattern)
        for collection in collections:
            try:
                dataset_refs = self.registry.queryDatasets(
                    pattern, collections=collection
                )
            except OSError():
                print("No datasets found for: ", collection)
                continue
                #
            k = 0
            lc = 0  # task counter
            task_size = dict()
            task_refs = dict()
            _refs = list()
            first = True
            for i, dataref in enumerate(dataset_refs):
                k += 1
                taskname = str(dataref).split("_")[0]
                if taskname not in task_size:
                    if first:
                        curr_task = taskname
                        first = False
                    else:
                        task_refs[curr_task] = _refs
                        curr_task = taskname
                    lc = 0
                    task_size[taskname] = 1
                    _refs = [dataref]
                    #            print(dataref)
                else:
                    task_size[taskname] += 1
                    lc += 1
                    if lc < self.maxtask:
                        _refs.append(dataref)
                    #                    else:
                task_refs[taskname] = _refs
            self.collData[collection] = task_refs
            self.collSize[collection] = task_size

    def make_table_from_csv(self, buffer, out_file, index_name, comment):
        """Create table from csv file
        
        Parameters
        ----------
        buffer : TODO
            TODO
        out_file : TODO
            TODO
        index_name : TODO
            TODO
        comment : TODO
            TODO
            
        Returns
        -------
        newbody : `str`
            TODO
        """
        
        newbody = comment + "\n"
        newbody += out_file + "\n"
        lines = buffer.split("\n")
        comma_matcher = re.compile(r",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)")
        i = 0
        for line in lines:
            if i == 0:
                tokens = line.split(",")
                #                print(tokens)  #line.split(',')
                line = "|" + index_name
                for ln in range(1, len(tokens)):
                    line += "||" + tokens[ln]
                line += "||\r\n"
            elif i >= 1:
                tokens = comma_matcher.split(line)
                #                print(tokens)
                line = "|"
                for token in tokens:
                    line += token + "|"
                line = line[:-1]
                line += "|\r\n"
            newbody += line
            i += 1
        newbody = newbody[:-2]
        tb_file = open("/tmp/" + out_file + "-" + self.Jira + ".txt", "w")
        print(newbody, file=tb_file)
        return newbody

    def run(self):
        """TODO
        """
        
        collections = self.search_collections()
        """Recreate Butler and registry """
        self.butler = Butler(self.REPO_ROOT, collections=collections)
        self.registry = self.butler.registry
        self.gettaskdata(collections)
        """
        Process a list of datarefs, extracting the per-task resource usage
        info from the `*_metadata` yaml files.
        """
        verbose = True
        columns = ("detector", "tract", "patch", "band", "visit")
        """ create temporary file for parsing metadata yaml """
        if not os.path.exists("/tmp/tempTask.yaml"):
            myfile = open("/tmp/tempTask.yaml", "w")
            testdict = {"test": ""}
            yaml.dump(testdict, myfile)
        for collection in collections:
            task_data = self.collData[collection]
            task_size = self.collSize[collection]
            task_res = dict()
            for task in task_data:
                data = defaultdict(list)
                data_refs = task_data[task]
                for i, dataref in enumerate(data_refs):
                    if verbose:
                        if i % 100 == 0:
                            sys.stdout.write(".")
                            sys.stdout.flush()
                    try:
                        refyaml = self.butler.getURI(dataref, collections=collection)
                    except ValueError:
                        print("Yaml file not found skipping")
                        continue
                    dest = ButlerURI("/tmp/tempTask.yaml")
                    buri = ButlerURI(refyaml)
                    if not buri.exists():
                        print("The file do not exists")
                    data_id = dict(dataref.dataId)
                    #            print("dataId ",dataId)
                    if "visit" not in data_id and "exposure" in data_id:
                        data_id["visit"] = data_id["exposure"]
                    for column in columns:
                        data[column].append(data_id.get(column, None))
                    """Copy metadata.yaml to local temp yaml """
                    dest.transfer_from(buri, "copy", True)
                    """parse results """
                    results = self.parse_metadata_yaml(yaml_file="/tmp/tempTask.yaml")
                    if (
                            results.get("EndCpuTime", None) is None
                            and results.get("endCpuTime", None) is not None
                    ):
                        cpu_time = float(results.get("endCpuTime", None))
                    else:
                        cpu_time = float(results.get("EndCpuTime", None))
                    data["cpu_time"].append(cpu_time)
                    data["maxRSS"].append(results.get("MaxResidentSetSize", None))
                    if results.get("timestamp", None) is None:
                        data["startTime"].append(
                            strftime("%Y-%m-%d %H:%M:%S", gmtime())
                        )
                    else:
                        data["startTime"].append(results.get("timestamp", None))
                task_res[task] = data
            key = self.CollKeys[collection]
            for task in task_res:
                self.workflowRes[key + "_" + task] = self.make_sum(
                    task_size[task], task_res[task]
                )
            """Now create pandas frame to display results"""
        dt = dict()
        all_tasks = list()
        camp_cpu = 0.0
        camp_rss = 0.0
        camp_jobs = 0
        camp_cpu_per_task = 0.0
        for task in self.workflowRes:
            all_tasks.append(task)
            dt[task] = self.workflowRes[task]
            camp_cpu += float(self.workflowRes[task]["cpu-hours"])
            camp_jobs += self.workflowRes[task]["nQuanta"]
            if float(self.workflowRes[task]["MaxRSS GB"]) >= camp_rss:
                camp_rss = float(self.workflowRes[task]["MaxRSS GB"])
        all_tasks.append("Campaign")
        utime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        camp_data = {
            "nQuanta": int(camp_jobs),
            "startTime": utime,
            "cpu sec/job": camp_cpu_per_task,
            "cpu-hours": float(camp_cpu),
            "MaxRSS GB": float(camp_rss),
        }
        dt["campaign"] = camp_data
        for ttype in dt:
            task = dt[ttype]
            task["cpu-hours"] = str(datetime.timedelta(seconds=task["cpu-hours"]))
            if isinstance(task["cpu sec/job"], float):
                task["cpu sec/job"] = round(task["cpu sec/job"], 2)
            task["MaxRSS GB"] = round(task["MaxRSS GB"], 2)
        print("")
        pd.set_option("max_colwidth", 500)
        pd.set_option("precision", 1)
        _taskids = dict()
        ttypes = list()
        stat_list = list()
        """Let's sort entries by start time"""
        for ttype in dt:
            task = dt[ttype]
            utime = task["startTime"]
            task["startTime"] = utime
            tokens = utime.split(".")
            utime = tokens[0]
            task["startTime"] = utime
            utime = datetime.datetime.strptime(utime, "%Y-%m-%d %H:%M:%S").timestamp()
            _taskids[ttype] = utime
        #
        for tt in dict(sorted(_taskids.items(), key=lambda item: item[1])):
            ttypes.append(tt)
            stat_list.append(dt[tt])

        data_frame = pd.DataFrame(stat_list, index=ttypes)
        fig, ax = plt.subplots(figsize=(25, 35))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, data_frame, loc="upper right")
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.auto_set_column_width(col=list(range(len(data_frame.columns))))
        tabla.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabla.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/butlerStat-" + self.Jira + ".png", transparent=True)
        plt.show()
        """ print the table """
        print(tabulate(data_frame, headers="keys", tablefmt="fancy_grid"))
        cs_buf = data_frame.to_csv(index=True)
        table_name = "butlerStat"
        index_name = " Workflow Task "
        comment = " Campaign Butler statistics " + self.Jira
        self.make_table_from_csv(cs_buf, table_name, index_name, comment)


if __name__ == "__main__":
    print(sys.argv)
    nbpar = len(sys.argv)
    if nbpar < 1:
        print("Usage: GetButlerStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <yamlFile> - yaml file containing input parameters")
        sys.exit(-2)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:", ["yamlFile="])
    except getopt.GetoptError:
        print("Usage: GetButlerStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <yamlFile> - yaml file containing input parameters ")
        sys.exit(2)
    yaml_flag = 0

    for opt, arg in opts:
        print("%s %s" % (opt, arg))
        if opt == "-h":
            print("Usage: GetButlerStat.py <required inputs>")
            print("  Required inputs:")
            print("  -f <yamlFile> - yaml file containing input parameters ")
            print(" The yaml file format as following:")
            print(
                "Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml \n",
                "Jira: PREOPS-707\n",
                "collType: 2.2i\n",
                "workNames: not used now\n",
                "maxtask: 100",
            )
            sys.exit(2)
        elif opt in ("-f", "--yamlFile"):
            yaml_flag = 1
            inpFile = arg
    inpsum = yaml_flag
    if inpsum != 1:
        print("Usage: GetButlerStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <yamlFile> - yaml file containing input parameters ")
        sys.exit(-2)
    with open(inpFile) as pf:
        inpars = yaml.safe_load(pf)
    butler_uri = inpars["Butler"]
    GBS = GetButlerStat(**inpars)
    GBS.set_butler(butler_uri)
    GBS.run()
    print("End with GetButler Stat.py")
