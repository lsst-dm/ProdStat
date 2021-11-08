#!/usr/bin/env python
""" This is an example program how to read production statistics
 from PanDa database"""
import sys
import json
from urllib.request import urlopen
import time
from time import sleep, gmtime, strftime
import datetime
import getopt
import math
import yaml
from tabulate import tabulate
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table
class GetPanDaStat:
    def __init__(self, inpf):
        """Get parameters - workflow keys from yaml file """
        with open(inpf) as pf:
            inpars = yaml.safe_load(pf)
        self.Butler = inpars['Butler']
        self.workKeys = inpars['workflows']
        self.Jira = inpars['Jira']
        self.maxtask = int(inpars['maxtask'])  # not used
        """convert keys to lowercase as it is in PanDa"""
        for i in range(len(self.workKeys)):
            self.workKeys[i] = str(self.workKeys[i]).lower()
        print(" Collecting information for Jira ticket ", self.Jira)
        print("with workflows:", self.workKeys)
        self.workflows = {}
        for key in self.workKeys:
            self.workflows[key] = []
        self.wfInfo = dict()  # workflow status
        self.taskCounts = dict()  # number of tasks of given type
        self.allTasks = dict()  # info about tasks
        self.allJobs = dict()  # info about jobs
        self.wfTasks = dict()  # tasks per workflow
        self.taskStat = dict()
        self.allStat = dict()  # general statistics

    def getWorkflows(self):
        """First lets get all workflows with given keys """
        wfdata = self.queryPanda(urlst="http://panda-doma.cern.ch/idds/wfprogress/?json")
        for wf in self.workKeys:
            for i in range(len(wfdata)):
                r_name = wfdata[i]['r_name']
                if wf in r_name:
                    self.workflows[wf].append(wfdata[i])
        for key in self.workKeys:
            workflow = self.workflows[key]
            for wf in workflow:
                r_status = wf['r_status']
                total_tasks = wf['total_tasks']
                total_files = wf['total_files']
                remaining_files = wf['remaining_files']
                processed_files = wf['processed_files']
                task_statuses = wf['tasks_statuses']
                finished = task_statuses['Finished']
                try:
                    subfinished = task_statuses['SubFinished']
                except:
                    subfinished = 0
                try:
                    failed = task_statuses['Failed']
                except:
                    failed = 0
                if key not in self.wfInfo:
                    self.wfInfo[key] = {'status': r_status, 'ntasks': float(total_tasks),
                                        'nfiles': float(total_files),
                                        'remaining files': float(remaining_files),
                                        'processed files': float(processed_files),
                                        'task_finished': float(finished),
                                        'task_failed': float(failed),
                                        'task_subfinished': float(subfinished)}
    """Select tasks for given workflow """
    def getWfTasks(self, workflow):
        urls = workflow['r_name']
#        print(urls)
        tasks = self.queryPanda(urlst="http://panda-doma.cern.ch/tasks/?taskname=" + urls + "*&days=120&json")
        return tasks

    """extract data we need from task dictionary """
    def getTaskInfo(self, task, result):
        data = dict()
#        job = result['job']
        jeditaskid = task['jeditaskid']
        """ Now select a number of jobs to calculate everage cpu time and max Rss """
        uri = "http://panda-doma.cern.ch/jobs/?jeditaskid=" + str(jeditaskid) + \
                "&limit="+str(self.maxtask) + "&jobstatus=finished&json"
#        print(' uri=',uri)
        jobsdata = self.queryPanda(urlst=uri)
        jobs = jobsdata['jobs']
        njobs = len(jobs)
#        print('njobs=',njobs)
        corecount = 0
        maxRss = 0
        duration = 0
        attempts = 0
        starttime = float(round(time.time()))
        if njobs > 0:
            for jb in jobs:
                corecount += jb['actualcorecount']
                duration += jb['durationsec']
                attempts += jb['attemptnr']
                tokens = jb['starttime'].split('T')
                startst = tokens[0] + ' ' + tokens[1]  # get rid of T in the date string
                taskstart = datetime.datetime.strptime(startst, "%Y-%m-%d %H:%M:%S").timestamp()
                if starttime >= taskstart:
                    starttime = taskstart
                if maxRss <= jb['minramcount']:
                    maxRss = jb['minramcount']
            corecount = float(corecount / njobs)
            duration = float(duration / njobs)
            attemptnr = float(attempts / njobs)
        else:
            return data
        """select first good job """
        for jb in jobs:
            if jb['jobstatus'] != 'failed':
                break
        """Fill data with the firs good job """
        dsinfo = task['dsinfo']
        data['jeditaskid'] = task['jeditaskid']
        data['jobname'] = jb['jobname']
        data['taskname'] = task['taskname']
        data['status'] = task['status']
        data['attemptnr'] = int(attemptnr)
        data['actualcorecount'] = jb['actualcorecount']
        data['starttime'] = task['starttime']
        tokens = task['endtime'].split('T')
        data['endtime'] = tokens[0] + ' ' + tokens[1] # get rid of T in the date string
        if task['starttime'] == None:
            task['starttime'] = tokens[0] + ' ' + tokens[1]
        data['starttime'] = task['starttime']
        data['maxattempt'] = jb['maxattempt']
        data['basewalltime'] = task['basewalltime']
        data['cpuefficiency'] = task['cpuefficiency']
        data['maxdiskcount'] = jb['maxdiskcount']
        data['maxdiskunit'] = jb['maxdiskunit']
        data['cpuconsumptiontime'] = duration
        data['jobstatus'] = jb['jobstatus']
        tokens = jb['starttime'].split('T')
        data['jobstarttime'] = tokens[0] + ' ' + tokens[1]  # get rid of T in the date string
        tokens = jb['endtime'].split('T')
        data['jobendtime'] = tokens[0] + ' ' + tokens[1]  # get rid of T in the date string
        taskstart = datetime.datetime.strptime(data['starttime'], "%Y-%m-%d %H:%M:%S").timestamp()
#        jobstart = datetime.datetime.strptime(data['jobstarttime'], "%Y-%m-%d %H:%M:%S").timestamp()
        jobstart = starttime
        taskend = datetime.datetime.strptime(data['endtime'], "%Y-%m-%d %H:%M:%S").timestamp()
        taskduration = taskend - taskstart
        jobduration = taskend - jobstart
        data['ncpus'] = corecount
        data['taskduration'] = jobduration
        data['exeerrorcode'] = jb['exeerrorcode']
        data['nfiles'] = dsinfo['nfiles']
#        data['Rss'] = task['ramcount'] # in MB
        data['Rss'] = maxRss
#        print("Task name ",task['taskname'],' attempt # ',data['attemptnr'])
#        print("Task start ",task['starttime'],' task end ',task['endtime'],
#                ' job start ',jb['starttime'],' job end ',jb['endtime'])
#        print("duration task ", taskduration," duration job ",jobduration)
        return data

    """given list of tasks get statistics for each task type """
    def getTaskData(self, key, tasks):
        taskData = list()
        taskNames = list()
        taskTypes = dict()
        taskids = dict()
        """Let's sort tasks with jeditaskid """
        i = 0
        for task in tasks:
            _id = task['jeditaskid']
            taskids[_id] = i
            i += 1
        for _id in sorted(taskids):
            tind = taskids[_id]
            task = tasks[tind]
            comp = key.upper()
            taskname = task['taskname'].split(comp)[1]
            tokens = taskname.split('_')
            name = ''
            for i in range(1, len(tokens) - 1):
                name += (tokens[i] + '_')
            taskname = name[:-1]
            taskid = task['jeditaskid']
            uri = "http://panda-doma.cern.ch/job?pandaid=" + str(taskid) + "&json"
#            print(uri)
            result = self.queryPanda(urlst=uri)
            data = self.getTaskInfo(task, result)
            if len(data) == 0:
                print("The job ", taskname, " is failed")
                continue
            jobname = data['jobname'].split('Task')[0]
            taskname = data['taskname']
            comp = key.upper()
            taskname = taskname.split(comp)[1]
            tokens = taskname.split('_')
            name = ''
            for i in range(1, len(tokens)-1):
                name += (tokens[i] + '_')
            taskname = name[:-1]
            data['taskname'] = taskname
            data['jobname'] = jobname
            if jobname not in self.allJobs:
                self.allJobs[jobname] = []
            if taskname not in self.allJobs[jobname]:
                self.allJobs[jobname].append(taskname)
            data['walltime'] = data['taskduration']
            taskData.append(data)
        """Now create a list of task types"""
        for data in taskData:
            name = data['taskname']
            if name not in self.taskCounts:
                self.taskCounts[name] = 0
                self.allTasks[name] = list()
                taskNames.append(name)
        """Now create a list of tasks for each task type """
        for taskname in taskNames:
            tasklist = list()
            for task in taskData:
                if taskname == task['taskname']:
                    tasklist.append(task)
                    if taskname in self.allTasks:
                        self.allTasks[taskname].append(task)
                        self.taskCounts[taskname] += 1
            taskTypes[taskname] = tasklist
        return taskTypes

    """Select finished and sub finished workflow tasks """
    def getTasks(self):
        for key in self.workKeys:
            self.wfTasks[key] = list()
            _workflows = self.workflows[key]
            for wf in _workflows:
                if str(wf['r_status']) == 'finished' or str(wf['r_status']) == 'subfinished':
                    """ get tasks for this workflow """
                    tasks = self.getWfTasks(wf)
                    """get data for each task """
                    taskTypes = self.getTaskData(key, tasks)
                    self.wfTasks[key].append(taskTypes)
#            _tasknames = taskTypes.keys()
#            print('\n',key, ' ', _tasknames,'\n')
    @staticmethod
    def queryPanda(urlst):
        success = False
        ntryes = 0
        result = dict()
        while (not success) or (ntryes >= 5):
            try:
                with urlopen(urlst) as url:
                    result = json.loads(url.read().decode())
                    success = True
            except:
                print("failed with ", urlst, " retrying")
                print("ntryes=", ntryes)
                success = False
                ntryes += 1
                sleep(2)
        sys.stdout.write('.')
        sys.stdout.flush()
        return result

    def getAllStat(self):
        wfWall = 0
        wfDisk = 0
        wfCores = 0
        wfRss = 0
        wfDuration = 0.
        wfNfiles = 0
        wfParallel = 0
        campStart = ''
        campEnd = ''
        self.allStat = dict()
        for ttype in self.allTasks:
            self.allStat[ttype] = dict()
            tasks = self.allTasks[ttype]
            ntasks = len(tasks)
            itasks = self.taskCounts[ttype]
            cpuconsumption = 0
            walltime = 0
            cpuefficiency = 0
            corecount = 0
            maxdiskcount = 0
            duration = 0.
            taskduration = 0.
            nfiles = 0
            maxRss = 0
            attempts = 0.
            maxdiskunit = 'MB'
            ntasks = len(tasks)
            for i in range(ntasks):
                walltime += int(tasks[i]['walltime'])
                cpuconsumption += int(tasks[i]['cpuconsumptiontime'])
                cpuefficiency += int(tasks[i]['cpuefficiency'])
                maxdiskcount += int(tasks[i]['maxdiskcount'])
                duration += float(tasks[i]['cpuconsumptiontime'])
                attempts += tasks[i]['attemptnr']
                starttime = tasks[i]['starttime']
                endtime = tasks[i]['endtime']
                taskduration += tasks[i]['taskduration']
                corecount += int(tasks[i]['actualcorecount'])
                nfiles = int(tasks[i]['nfiles'])
                rss = tasks[i]['Rss']
                if maxRss <= rss:
                    maxRss = rss
            taskduration = taskduration/ntasks
            walltimePJ = cpuconsumption/ntasks
            walltime = walltimePJ * nfiles
            maxdiskPJ = maxdiskcount / ntasks
            diskcount = maxdiskPJ * nfiles
            corecountPJ = corecount / ntasks
            corecount = corecountPJ * nfiles
            cpueffPJ = cpuefficiency / ntasks
            nparallel = int(math.ceil(walltime / taskduration))
            if nparallel < 1:
                nparallel = 1
            wfDuration += taskduration
            wfWall += walltime
            wfDisk += diskcount
            wfCores += nparallel
            wfNfiles += nfiles
            if wfRss <= maxRss:
                wfRss = maxRss
            self.allStat[ttype] = {'nQuanta': float(nfiles), 'starttime':starttime,
                                   'wallclock': str(datetime.timedelta(seconds=taskduration)),
                                   'cpu sec/job': float(walltimePJ),
                                   'cpu-hours': str(datetime.timedelta(seconds=walltime)),
                                   'est. parallel jobs': nparallel}
#                                   'coresPJ': float(corecountPJ),
#                                   'cores': float(corecount)}
#                                   'requested memory MB': float(maxRss)}
        wfParallel = int(math.ceil(wfWall / wfDuration))
        self.allStat['Campaign'] = {'nQuanta': float(wfNfiles),
                                    'starttime': strftime("%Y-%m-%d %H:%M:%S", gmtime()),
                                    'wallclock': str(datetime.timedelta(seconds=wfDuration)),
                                    'cpu sec/job': '-',
                                    'cpu-hours':  str(datetime.timedelta(seconds=wfWall)),
                                    'est. parallel jobs': wfParallel}
#                                    'starttime': ' ',
#                                    'endtime': ' ',
#                                    'coresPJ': 0.,
#                                    'cores': float(wfCores)}
#                                    'requested memory MB': float(wfRss)}
    def run(self):
        self.getWorkflows()
        self.getTasks()
        self.getAllStat()
        print("workflow info")
        wfInd = list()

        wfList = list()
#        wfIndF = open('./wfInd.txt','w')
        for key in self.wfInfo:
            wfInd.append(key)
            wfList.append(self.wfInfo[key])
#            print(key,file=wfIndF)
#        wfIndF.close()
        pd.set_option('max_colwidth', 500)
        pd.set_option('precision', 1)
        dataFrame = pd.DataFrame(wfList, index=wfInd)
        fig,ax = plt.subplots(figsize=(15,30))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabula = table(ax, dataFrame, loc='upper right')
        tabula.auto_set_font_size(False)  # Activate set fontsize manually
        tabula.auto_set_column_width(col=list(range(len(dataFrame.columns))))
        tabula.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabula.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/pandaWfStat-"+self.Jira+".png", transparent=True)
        plt.show()
        print(tabulate(dataFrame, headers='keys', tablefmt='fancy_grid'))
        """ write csv file with the data """
        compression_opts = dict(method='zip',archive_name='pandaWfTab-'+self.Jira+'.csv')
        dataFrame.to_csv('./pandaWfTab-'+self.Jira+'.zip', index=True, compression=compression_opts)
#       """ print the table in a file"""
#        fWf = open('./pandaWfTab.txt','w')
#        print(tabulate(dataFrame, headers='keys', tablefmt='fancy_grid'), file=fWf)
#        fWf.close()
#        print()
#        print(" allStat")
        _taskids = dict()
        ttypes = list()
        statList = list()
#        fPdTaskF = open('./pandaTaskInd.txt', 'w')
        """Let's sort entries by start time"""
        for ttype in self.allStat:
            utime = self.allStat[ttype]['starttime']
            print('ttype=',ttype,' starttime=',utime)
            utime = datetime.datetime.strptime(utime, "%Y-%m-%d %H:%M:%S").timestamp()
            _taskids[ttype] = utime
#
        for ttype in dict(sorted(_taskids.items(), key=lambda item: item[1])):
            ttypes.append(ttype)
            statList.append(self.allStat[ttype])
#            print(ttype, file=fPdTaskF)
#        fPdTaskF.close()
        fig, bx = plt.subplots(figsize=(20, 35))  # set size frame
        bx.xaxis.set_visible(False)  # hide the x axis
        bx.yaxis.set_visible(False)  # hide the y axis
        bx.set_frame_on(False)  # no visible frame, uncomment if size is ok
        dfs = pd.DataFrame(statList, index=ttypes)
        tabula = table(bx, dfs, loc='upper right')
        tabula.auto_set_font_size(False)  # Activate set fontsize manually
        tabula.auto_set_column_width(col=list(range(len(dataFrame.columns))))
        tabula.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabula.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/pandaStat-"+self.Jira+".png", transparent=True)
        plt.show()
        print(tabulate(dfs, headers='keys', tablefmt='fancy_grid'))
#        """ print the table to text file """
#        pandaDataF = open('./pandaStat.txt', 'w')
#        print(tabulate(dfs, headers='keys', tablefmt='fancy_grid'), file=pandaDataF)
#        pandaDataF.close()
#        """Save data frame for later use"""
#        compression_opts = dict(method='zip',archive_name='pandaTab-'+self.Jira+'.csv')
#        dfs.to_csv('./pandaTab-'+self.Jira+'.zip', index=True, compression=compression_opts)

if __name__ == "__main__":
    print(sys.argv)
    nbpar = len(sys.argv)
    if nbpar < 1:
        print("Usage: GetPanDaStat.py <required inputs>")
        print("Required inputs:")
        print("-f <inputYaml> - yaml file with input parameters")
        sys.exit(-2)

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:", ["inputYaml="])
    except getopt.GetoptError:
        print("Usage: GetPanDaStat.py <required inputs>")
        print("Required inputs:")
        print("-f <inputYaml> - yaml file with input parameters")
        sys.exit(2)
    f_flag = 0
    inpF = ''
    for opt,arg in opts:
        print ("%s %s"%(opt, arg))
        if opt == "-h":
            print("Usage: GetPanDaStat.py <required inputs>")
            print("  Required inputs:")
            print("  -f <inputYaml> - yaml file with input parameters")
            print("The yaml file format as following:")
            print("Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml \n",
            "Jira: PREOPS-707\n",
            "workflows:\n",
            "- 20211001T200704Z\n",
            "- 20211003T045430Z\n",
            "- 20211003T164354Z\n",
            "maxtask: 100\n")
            sys.exit(2)
        elif opt in ("-f", "--inputYaml"):
            f_flag = 1
            inpF = arg
    inpsum = f_flag
    if inpsum != 1:
        print("Usage: GetPanDaStat.py <required inputs>")
        print("  Required inputs:")
        print("  -f <inputYaml> - yaml file with input parameters")
        sys.exit(-2)
    # Create new threads
    GPS = GetPanDaStat(inpF)
    GPS.run()
    print("End with GetPanDaStat.py")

