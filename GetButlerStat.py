#!/usr/bin/env python
# coding: utf-8
import sys
import re
import os
from collections import defaultdict
import yaml
import getopt
from tabulate import tabulate
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.plotting import table
from lsst.daf.butler import Butler
from lsst.daf.butler import ButlerURI
from lsst.daf.base import PropertySet

class GetButlerStat:
    def __init__(self, parFile):
        inpars = {}
        with open(parFile) as pf:
            inpars = yaml.safe_load(pf)
        self.Butler = inpars['Butler']
        self.workflows = inpars['workflows']
        self.Jira = inpars['Jira']
        self.maxtask = int(inpars['maxtask'])
        print(" Collecting information for Jira ticket ",self.Jira)
        print("with workflows:",self.workflows)
        self.REPO_ROOT = self.Butler
        # REPO_ROOT = 's3://butler-us-central1-panda-dev/hsc/butler.yaml'
        self.butler = Butler(self.REPO_ROOT)
        self.registry = self.butler.registry
        self.workflowRes = {}

    def parse_metadata_yaml(self,yaml_file):
        """
        Parse the runtime and RSS data in the metadata yaml file created
        by the lsst.pipe.base.timeMethod decorator as applied to pipetask
        methods.
        """
        time_types = 'Cpu User System'.split()
        min_fields = [f'Start{_}Time' for _ in time_types] + [f'start{_}Time' for _ in time_types]
        max_fields = [f'End{_}Time' for _ in time_types] + [f'end{_}Time' for _ in time_types] + ['MaxResidentSetSize']
        results = dict()
        with open(yaml_file) as fd:
            md = yaml.safe_load(fd)
        methods = list(md.keys())
#    print(md)
#    print("Methods ",methods)
        for method in methods:
            for key, value in md[method].items():
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
        " Select collections "
        collections=[]
        prepos = self.Jira
        searchcolls= self.workflows

        for searchcoll in searchcolls:
            for c in sorted(self.registry.queryCollections()):
                if prepos in str(c) and searchcoll in str(c):
                    collections.append(c)
        " Now we have all collections with PREPOS in the name"
        print (collections)
        print(' ')
        return collections

    def run(self):
        collections = self.searchCollections()
        print("collections:",collections)
        " Recreate Butler and registry "
        self.butler = Butler(self.REPO_ROOT, collections=collections)
        self.registry = self.butler.registry
        self.gettaskdata(collections)
        """
        Process a list of datarefs, extracting the per-task resource usage
        info from the `*_metadata` yaml files.
        """
        verbose = True
        columns = ('detector', 'tract', 'patch', 'band', 'visit')
        " create temporary file for parsing metadata yaml "
        if not os.path.exists('/tmp/MyFile.yaml'):
            myfile = open('/tmp/MyFile.yaml', 'w')
            testdict = {'test': ''}
            yaml.dump(testdict, myfile)
        for collection in collections:
            taskData = self.collData[collection]
            taskSize = self.collSize[collection]
            taskRes = {}
            for task in taskData:
                data = defaultdict(list)
                dataRefs = taskData[task]
                for i, dataref in enumerate(dataRefs):
                    if verbose:
                        if i % 20 == 0:
                            sys.stdout.write('.')
                            sys.stdout.flush()
                    try:
                        refyaml = self.butler.getURI(dataref, collections=collection)
                    except:
                        print('Yaml file not found skiping')
                        continue
                    dest = ButlerURI('/tmp/MyFile.yaml')
                    buri = ButlerURI(refyaml)
                    if not buri.exists():
                        print('The file do not exists')
                    dataId = dict(dataref.dataId)
                    #            print("dataId ",dataId)
                    if 'visit' not in dataId and 'exposure' in dataId:
                        dataId['visit'] = dataId['exposure']
                    for column in columns:
                        data[column].append(dataId.get(column, None))
                    " Copy metadata.yaml to local temp yaml "
                    dest.transfer_from(buri, 'copy', True)
                    " parse results "
                    results = self.parse_metadata_yaml('/tmp/MyFile.yaml')
#                    cpu_time = 0.
                    if results.get('EndCpuTime', None) is None and results.get('endCpuTime', None) is not None:
                        cpu_time = float(results.get('endCpuTime', None))
                    else:
                        cpu_time = float(results.get('EndCpuTime', None))
                    data['cpu_time'].append(cpu_time)
#                    data['cpu_time'].append(results.get('EndCpuTime', None))
                    data['maxRSS'].append(results.get('MaxResidentSetSize', None))

                taskRes[task] = data

            for task in taskRes:
                self.workflowRes[task] = self.makeSum(taskSize[task],taskRes[task])

            " Now create pandas frame to"
#        print(self.workflowRes)
        dt = []
        allTasks = []
        campCpu = 0.
        campRss = 0.
        campJobs = 0
        campCpuPerTask = 0.
        for task in self.workflowRes:
            allTasks.append(task)
            dt.append(self.workflowRes[task])
            campCpu += float(self.workflowRes[task]['totalCPU'])
            campJobs += self.workflowRes[task]['N tasks']
            campRss += float(self.workflowRes[task]['MaxRSS'])
        allTasks.append('Campaign')
        campData = {'N tasks':int(campJobs),'cpuPerTask':float(campCpuPerTask),'totalCPU':float(campCpu),'MaxRSS':float(campRss)}
        dt.append(campData)
        print('')
        dataFrame = pd.DataFrame(dt,index=allTasks)
        fig, ax = plt.subplots(figsize=(15, 20))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabla = table(ax, dataFrame, loc='upper right', colWidths=[0.18] * len(dataFrame.columns))  # where df is your data frame
        tabla.auto_set_font_size(False)  # Activate set fontsize manually
        tabla.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabla.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/mytable.png", transparent=True)

#        ax = plt.subplot(111, frame_on=False)  # no visible frame
#        ax.xaxis.set_visible(False)  # hide the x axis
#        ax.yaxis.set_visible(False)  # hide the y axis

#        table(ax, dataFrame)#

        print(tabulate(dataFrame,  headers='keys', tablefmt='psql'))


    " Calculate mean and total cpu usage and MaxRSS "
    def makeSum(self,taskSize,taskRes):
        cputime = taskRes['cpu_time']
        MaxRSS = taskRes['maxRSS']
        ts = min(int(taskSize),self.maxtask)
#        print(taskRes)
        if cputime[0] is not None:
            cpuSum = 0.0
            for t in cputime: cpuSum+=float(t)
            cpuPerTask = float(cpuSum/ts)
            totalCpu = float(cpuPerTask*int(taskSize))
        else:
            cpuPerTask =0.
            totalCpu = 0.
        maxS = 0.
        for s in MaxRSS:
            if float(s) >= maxS:
                maxS = float(s)

        return {'N tasks':int(taskSize),'cpuPerTask':float(cpuPerTask),'totalCPU':float(totalCpu),'MaxRSS':float(maxS)}


    def gettaskdata(self,collections):
        """ We can collect datasets and data IDs
        for each collection and create subset of ID's for each
             process type"""
        datatype_pattern='.*_metadata'
        pattern = re.compile(datatype_pattern)
        self.collSize = dict()
        self.collData = dict()

        for collection in collections:
            datasetRefs = []
            datasetRefs = self.registry.queryDatasets(pattern, collections=collection)

            taskData = {}
            oldTask = ''
#
            print('collection=',collection)
            k=0
            l=0
            taskSize = {}
            taskRefs = {}
            _refs = []
            first = True
            for i, dataref in enumerate(datasetRefs):
                k+=1
                taskname = str(dataref).split('_')[0]

                if taskname not in taskSize:
                    if first:
                        curr_task = taskname
                        first = False
                    else:
                        taskRefs[curr_task] = _refs
                        curr_task = taskname
                    l=0
                    taskSize[taskname] = 1
                    _refs = []
                    _refs.append(dataref)
#            print(dataref)
                else:
                    taskSize[taskname] +=1
                    l+=1
                    if l < self.maxtask:
                        _refs.append(dataref)
#                    else:
            taskRefs[taskname] = _refs
            self.collData[collection] = taskRefs
            self.collSize[collection] = taskSize



# 
"""
    dest = ButlerURI('/home/kuropat/WORK/')
    dataset_type='calexp'
    find_first=True
    preserve_path=False
    clobber=True
    refs = list(butler.registry.queryDatasets(datasetType=dataset_type,
                                              collections=collections,
                                              where='',
                                              findFirst=find_first))
    print('refs=',refs)
#    log.info("Number of datasets matching query: %d", len(refs))

    transferred = butler.retrieveArtifacts(refs, destination=dest, transfer='copy',
                                           preserve_path=preserve_path, overwrite=clobber)
"""
if __name__ == "__main__":
    print(sys.argv)
    nbpar = len(sys.argv)
    if nbpar < 1:
        print("Usage: GetButlerStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <yamlFile> - yaml file containing input parameters")
        sys.exit(-2)

    try:
        opts, args = getopt.getopt(sys.argv[1:],"hf:",["yamlFile="])
    except getopt.GetoptError:
        print("Usage: GetButlerStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <yamlFile> - yaml file containing input parameters ")
        sys.exit(2)
    yaml_flag = 0

    for opt,arg in opts:
        print ("%s %s"%(opt,arg))
        if opt == "-h":
            print("Usage: GetButlerStat.py <required inputs>")
            print("  Required inputs:")
            print("  -f <yamlFile> - yaml file containing input parameters ")
            print("  -u <username> - user to get data for")
            sys.exit(2)
        elif opt in ("-f","--yamlFile"):
            yaml_flag = 1
            inpFile = arg

    inpsum = yaml_flag
    if inpsum != 1:
        print("Usage: GetButlerStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <yamlFile> - yaml file containing input parameters ")
        sys.exit(-2)
    # Create new threads
    GBS = GetButlerStat(inpFile)

    GBS.run()


    print( "End with GetButler Stat.py")
