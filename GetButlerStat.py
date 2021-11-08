#!/usr/bin/env python
# coding: utf-8
import sys
import re
import os
import datetime
import time
from time import sleep, gmtime, strftime
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
    def __init__(self, parfile):
        with open(parfile) as pf:
            inpars = yaml.safe_load(pf)
        self.Butler = inpars['Butler']
        self.workflows = inpars['workflows']
        self.Jira = inpars['Jira']
        self.maxtask = int(inpars['maxtask'])
        print(" Collecting information for Jira ticket ", self.Jira)
        print("with workflows:", self.workflows)
        self.REPO_ROOT = self.Butler
        # REPO_ROOT = 's3://butler-us-central1-panda-dev/hsc/butler.yaml'
        self.butler = Butler(self.REPO_ROOT)
        self.registry = self.butler.registry
        self.workflowRes = {}

    @staticmethod
    def parse_metadata_yaml(yaml_file):
        """
        Parse the runtime and RSS data in the metadata yaml file created
        by the lsst.pipe.base.timeMethod decorator as applied to pipetask
        methods.
        """
        time_types = 'Cpu User System'.split()
        min_fields = [f'Start{_}Time' for _ in time_types] + [f'start{_}Time' for _ in time_types]
        max_fields = [f'End{_}Time' for _ in time_types] + [f'end{_}Time' for _ in time_types] + ['MaxResidentSetSize']
        time_stamp = ['startUtc','prepUtc']
        results = dict()
        with open(yaml_file) as fd:
            md = yaml.safe_load(fd)
        methods = list(md.keys())
        for method in methods:
            for key, value in md[method].items():
                if key in time_stamp:
                    starts = value
                    if 'T' in value:
                        tokens = starts.split('T')
                        startst = tokens[0] + ' ' + tokens[1]  # get rid of T in the date string
                    if 'timestamp' not in results:
                        results['timestamp'] = startst
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

    def searchCollections(self):
        """ Select collections """
        collections = []
        prepos = self.Jira
        searchcolls = self.workflows

        for searchcoll in searchcolls:
            for c in sorted(self.registry.queryCollections()):
                if prepos in str(c) and searchcoll in str(c):
                    collections.append(c)
        " Now we have all collections with PREPOS in the name"
        return collections

        " Calculate mean and total cpu usage and MaxRSS "
    def makeSum(self, taskSize, taskRes):
        cputime = taskRes['cpu_time']
        MaxRSS = taskRes['maxRSS']
        time_start = taskRes['startTime']
        ts = min(int(taskSize), self.maxtask)
        if cputime[0] is not None:
            cpuSum = 0.0
            for t in cputime:
                cpuSum += float(t)
            cpuPerTask = float(cpuSum / ts)
            totalCpu = float(cpuPerTask * int(taskSize))
        else:
            cpuPerTask = 0.
            totalCpu = 0.
        maxS = 0.
        for s in MaxRSS:
            if float(s) >= maxS:
                maxS = float(s)
        return {'nQuanta': int(taskSize), 'startTime': time_start[0], 'cpu sec/job': float(cpuPerTask),
                'cpu-hours': float(totalCpu), 'MaxRSS MB': float(maxS / 1048576.)}

    def gettaskdata(self, collections):
        """ We can collect datasets and data IDs
        for each collection and create subset of ID's for each
        process type"""
        datatype_pattern = '.*_metadata'
        pattern = re.compile(datatype_pattern)
        self.collSize = dict()
        self.collData = dict()
        for collection in collections:
            try:
                datasetRefs = self.registry.queryDatasets(pattern, collections=collection)
            except:
                print("No datasets found for: ", collection)
                continue
                #
            print('collection=', collection)
            k = 0
            lc = 0  # task counter
            taskSize = dict()
            taskRefs = dict()
            _refs = list()
            first = True
            for i, dataref in enumerate(datasetRefs):
                k += 1
                taskname = str(dataref).split('_')[0]
                if taskname not in taskSize:
                    if first:
                        curr_task = taskname
                        first = False
                    else:
                        taskRefs[curr_task] = _refs
                        curr_task = taskname
                    lc = 0
                    taskSize[taskname] = 1
                    _refs = []
                    _refs.append(dataref)
                    #            print(dataref)
                else:
                    taskSize[taskname] += 1
                    lc += 1
                    if lc < self.maxtask:
                        _refs.append(dataref)
                    #                    else:
                taskRefs[taskname] = _refs
            self.collData[collection] = taskRefs
            self.collSize[collection] = taskSize

    def run(self):
        collections = self.searchCollections()
#        print("collections:",collections)
        """Recreate Butler and registry """
        self.butler = Butler(self.REPO_ROOT, collections=collections)
        self.registry = self.butler.registry
        self.gettaskdata(collections)
        """
        Process a list of datarefs, extracting the per-task resource usage
        info from the `*_metadata` yaml files.
        """
        verbose = True
        columns = ('detector', 'tract', 'patch', 'band', 'visit')
        """ create temporary file for parsing metadata yaml """
        if not os.path.exists('/tmp/tempTask.yaml'):
            myfile = open('/tmp/tempTask.yaml', 'w')
            testdict = {'test': ''}
            yaml.dump(testdict, myfile)
        for collection in collections:
            taskData = self.collData[collection]
            taskSize = self.collSize[collection]
            taskRes = dict()
            for task in taskData:
                data = defaultdict(list)
                dataRefs = taskData[task]
                for i, dataref in enumerate(dataRefs):
                    if verbose:
                        if i % 100 == 0:
                            sys.stdout.write('.')
                            sys.stdout.flush()
                    try:
                        refyaml = self.butler.getURI(dataref, collections=collection)
                    except:
                        print('Yaml file not found skipping')
                        continue
                    dest = ButlerURI('/tmp/tempTask.yaml')
                    buri = ButlerURI(refyaml)
                    if not buri.exists():
                        print('The file do not exists')
                    dataId = dict(dataref.dataId)
                    #            print("dataId ",dataId)
                    if 'visit' not in dataId and 'exposure' in dataId:
                        dataId['visit'] = dataId['exposure']
                    for column in columns:
                        data[column].append(dataId.get(column, None))
                    """Copy metadata.yaml to local temp yaml """
                    dest.transfer_from(buri, 'copy', True)
                    """parse results """
                    results = self.parse_metadata_yaml(yaml_file='/tmp/tempTask.yaml')
                    if results.get('EndCpuTime', None) is None and results.get('endCpuTime', None) is not None:
                        cpu_time = float(results.get('endCpuTime', None))
                    else:
                        cpu_time = float(results.get('EndCpuTime', None))
                    data['cpu_time'].append(cpu_time)
                    data['maxRSS'].append(results.get('MaxResidentSetSize', None))
                    if results.get('timestamp',None) is None:
                        data['startTime'].append(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
                    else:
                        data['startTime'].append(results.get('timestamp',None))
                taskRes[task] = data
            for task in taskRes:
                self.workflowRes[task] = self.makeSum(taskSize[task], taskRes[task])
            """Now create pandas frame to display results"""
        dt = dict()
        allTasks = list()
        campCpu = 0.
        campRss = 0.
        campJobs = 0
        campCpuPerTask = 0.
        for task in self.workflowRes:
            allTasks.append(task)
            dt[task] = self.workflowRes[task]
            campCpu += float(self.workflowRes[task]['cpu-hours'])
            campJobs += self.workflowRes[task]['nQuanta']
            if float(self.workflowRes[task]['MaxRSS MB']) >= campRss:
                campRss = float(self.workflowRes[task]['MaxRSS MB'])
        allTasks.append('Campaign')
        utime = strftime("%Y-%m-%d %H:%M:%S", gmtime())
        campData = {'nQuanta': int(campJobs), 'startTime':utime, 'cpu sec/job': campCpuPerTask,
                    'cpu-hours':float(campCpu), 'MaxRSS MB': float(campRss)}
        #str(datetime.timedelta(seconds=campCpu))
        dt['campaign'] = campData
        for ttype in dt:
            task = dt[ttype]
            task['cpu-hours'] = str(datetime.timedelta(seconds=task['cpu-hours']))
            if isinstance(task['cpu sec/job'], float):
                task['cpu sec/job'] = round(task['cpu sec/job'], 2)
            task['MaxRSS MB'] = round(task['MaxRSS MB'], 2)
        print('')
        pd.set_option('max_colwidth', 500)
        pd.set_option('precision', 1)
        _taskids = dict()
        ttypes = list()
        statList = list()
        #        fPdTaskF = open('./pandaTaskInd.txt', 'w')
        """Let's sort entries by start time"""
        for ttype in dt:
            task = dt[ttype]
            utime = task['startTime']
            task['startTime'] = utime
            tokens = utime.split('.')
            utime = tokens[0]
            task['startTime'] = utime
#            print('utime=', utime)
            utime = datetime.datetime.strptime(utime, "%Y-%m-%d %H:%M:%S").timestamp()
            _taskids[ttype] = utime
        #
        for ttype in dict(sorted(_taskids.items(), key=lambda item: item[1])):
            ttypes.append(ttype)
            statList.append(dt[ttype])

        dataFrame = pd.DataFrame(statList, index=ttypes)
        fig, ax = plt.subplots(figsize=(20, 30))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, dataFrame, loc='upper right')
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.auto_set_column_width(col=list(range(len(dataFrame.columns))))
        tabla.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabla.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/butlerStat-"+self.Jira + ".png", transparent=True)
        plt.show()
        """ print the table """
        print(tabulate(dataFrame,  headers='keys', tablefmt='fancy_grid'))
#        """ create index file for offline DataFrame creation """
#        f = open('./index.txt', 'w')
#        for task in allTasks:
#            print(task,file=f)
#        f.close()
#        """ Write DataFrame as csv file for later use """
#        compression_opts = dict(method='zip',archive_name='butlerStat-'+self.Jira+'.csv')
#        dataFrame.to_csv('./butlerStat-'+self.Jira+'.zip', index=True, compression=compression_opts)


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

    for opt,arg in opts:
        print("%s %s" %(opt, arg))
        if opt == "-h":
            print("Usage: GetButlerStat.py <required inputs>")
            print("  Required inputs:")
            print("  -f <yamlFile> - yaml file containing input parameters ")
            print(" The yaml file format as following:")
            print("Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml \n",
            "Jira: PREOPS-707\n",
            "workflows:\n",
            "- 20211001T200704Z\n",
            "- 20211003T045430Z\n",
            "- 20211003T164354Z\n",
            "maxtask: 100")
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
    #
    GBS = GetButlerStat(inpFile)
    GBS.run()
    print( "End with GetButler Stat.py")