#!/usr/bin/env python
""" This is an example program how to read production statistics
 from PanDa database"""
import sys
import json
import re
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
        self.collType = inpars['collType']
        self.workNames = inpars['workNames']
        self.Jira = inpars['Jira']
        self.maxtask = int(inpars['maxtask'])  # not used
        self.workKeys = list()
        """convert keys to lowercase as it is in PanDa"""
#        for i in range(len(self.workKeys)):
#            self.workKeys[i] = str(self.workKeys[i]).lower()
        print(" Collecting information for Jira ticket ", self.Jira)
#        print("with steps:", self.workNames)
        self.workflows = {}
#        for key in self.workKeys:
#            self.workflows[key] = []
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
        comp = str(self.Jira).lower()
        comp1 = str(self.collType)
        self.wfNames = dict()
        nwf = 0
        for wf in wfdata:
            r_name = wf['r_name']
            if comp in r_name and comp1 in r_name:
                key = str(r_name).split('_')[-1]
                self.workKeys.append(str(key))
                """ possible mapping to steps """
#               self.wfNames[str(key)] = self.workNames[nwf]
#                print(key,': ',self.wfNames[key])
                nwf += 1
        print("number of workflows =",nwf)
        if nwf == 0:
            print("No workflows to work with -- exiting")
            sys.exit(-256)
        for key in self.workKeys:
            self.workflows[key] = []
        for wfk in self.workKeys:
            for wf in wfdata:
                r_name = wf['r_name']
                if wfk in r_name:
                    self.workflows[wfk].append(wf)
#
        print("Selected workflows:",self.workflows)
#        print(self.wfNames)
        for key in self.workKeys:
            workflow = self.workflows[key]
            for wf in workflow:
                created = datetime.datetime.strptime(wf['created_at'], "%Y-%m-%d %H:%M:%S").timestamp()
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
                                        'task_subfinished': float(subfinished),
                                        'created': created}

    """Select tasks for given workflow (jobs)"""
    def getWfTasks(self, workflow):
        urls = workflow['r_name']
        tasks = self.queryPanda(urlst="http://panda-doma.cern.ch/tasks/?taskname=" + urls + "*&days=120&json")
        return tasks

    """extract data we need from task dictionary """
    def getTaskInfo(self, task):
        data = dict()
        jeditaskid = task['jeditaskid']
        """ Now select a number of jobs to calculate average cpu time and max Rss """
        uri = "http://panda-doma.cern.ch/jobs/?jeditaskid=" + str(jeditaskid) + \
                "&limit=" + str(self.maxtask) + "&jobstatus=finished&json"
        jobsdata = self.queryPanda(urlst=uri)
        """ list of jobs in the task """
        jobs = jobsdata['jobs']
        njobs = len(jobs)
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
        data['Rss'] = maxRss
        return data

    """given list of jobs get statistics for each job type """
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
            data = self.getTaskInfo(task)
            if len(data) == 0:
                print("No data for ", taskname)
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
            taskname = str(key) + '_' + taskname
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
                if str(wf['r_status']) == 'finished' or str(wf['r_status']) == 'subfinished' or str(wf['r_status']) == 'running':
                    """ get tasks for this workflow """
                    tasks = self.getWfTasks(wf)
                    """get data for each task """
                    taskTypes = self.getTaskData(key, tasks)
                    self.wfTasks[key].append(taskTypes)

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

        wfParallel = int(math.ceil(wfWall / wfDuration))
        self.allStat['Campaign'] = {'nQuanta': float(wfNfiles),
                                    'starttime': strftime("%Y-%m-%d %H:%M:%S", gmtime()),
                                    'wallclock': str(datetime.timedelta(seconds=wfDuration)),
                                    'cpu sec/job': '-',
                                    'cpu-hours':  str(datetime.timedelta(seconds=wfWall)),
                                    'est. parallel jobs': wfParallel}

    def highlight_status(value):
        if str(value) == 'failed':
            return ['background-color: read'] * 9
        elif str(value) == 'subfinisher':
            return ['background-color: yellow'] * 9
        else:
            return ['background-color: green'] * 9

    def highlight_greaterthan_0(self,s):
        if s.task_failed > 0.0:
            return ['background-color: red'] * 9
        elif s.task_subfinished > 0.0:
            return ['background-color: yellow'] * 9
        else:
            return ['background-color: white'] * 9

    def make_table_from_csv(self, buffer, outFile, indexName, comment):
        newbody = comment + '\n'
        newbody += outFile + '\n'
        lines = buffer.split('\n')
        COMMA_MATCHER = re.compile(r",(?=(?:[^\"']*[\"'][^\"']*[\"'])*[^\"']*$)")
        i = 0
        for line in lines:
            if i == 0:
                tokens = line.split(',')
                line = '|' + indexName
                for l in range(1, len(tokens)):
                    line += '||' + tokens[l]
                line += '||\r\n'
            elif i >= 1:
                tokens = COMMA_MATCHER.split(line)
                line = "|"
                for token in tokens:
                    line += token + '|'
                line = line[:-1]
                line += '|\r\n'
            newbody += line
            i += 1
        newbody = newbody[:-2]
        tbfile = open("/tmp/" + outFile + "-" + self.Jira + ".txt", 'w')
        print(newbody, file=tbfile)
        return newbody

    def make_styled_table(self, dataFrame, outFile):
        df_styled = dataFrame.style.apply(self.highlight_greaterthan_0, axis=1)
        df_styled.set_table_attributes('border="1"')
        df_html = df_styled.render()
        htfile = open("/tmp/" + outFile + "-" + self.Jira + ".html", 'w')
        print(df_html, file=htfile)
        htfile.close()

    def make_table(self, dataFrame, tableName,indexName,comment):
        fig,ax = plt.subplots(figsize=(20,35))  # set size frame
        ax.xaxis.set_visible(False)  # hide the x axis
        ax.yaxis.set_visible(False)  # hide the y axis
        ax.set_frame_on(False)  # no visible frame, uncomment if size is ok
        tabula = table(ax, dataFrame, loc='upper right')
        tabula.auto_set_font_size(False)  # Activate set fontsize manually
        tabula.auto_set_column_width(col=list(range(len(dataFrame.columns))))
        tabula.set_fontsize(12)  # if ++fontsize is necessary ++colWidths
        tabula.scale(1.2, 1.2)  # change size table
        plt.savefig("/tmp/" + tableName + "-" + self.Jira + ".png", transparent=True)
        plt.show()
        dataFrame.to_csv("/tmp/" + tableName + "-" + self.Jira + ".csv",index=True)
        csbuf = dataFrame.to_csv(index=True)
        self.make_table_from_csv(csbuf, tableName,indexName,comment)

    def run(self):
        self.getWorkflows()
        self.getTasks()
        self.getAllStat()
        print("workflow info")
        wfInd = list()

        wfList = list()
#        wfIndF = open('./wfInd.txt','w')
        """ Let sort datasets by creation time"""
        _dfids = dict()
        _dfkeys = list()
        wfList = list()
        for key in self.wfInfo:
            """wfInd.append(self.wfNames[key])"""
            utime = self.wfInfo[key]['created']
            _sttime = datetime.datetime.utcfromtimestamp(utime)
            self.wfInfo[key]['created'] = _sttime
            _dfids[key] = utime
#
        for key in dict(sorted(_dfids.items(), key=lambda item: item[1])):
            wfInd.append(str(key))
            _dfkeys.append(key)
            wfList.append(self.wfInfo[key])


        pd.set_option('max_colwidth', 500)
        pd.set_option('precision', 1)
        dataFrame = pd.DataFrame(wfList, index=wfInd)
        comment = """ workflow status """
        indexName = "workflow"
        tableName = "pandaWfStat"
        self.make_table(dataFrame, tableName, indexName,comment)
        self.make_styled_table(dataFrame, tableName)

        _taskids = dict()
        ttypes = list()
        statList = list()
        """Let's sort entries by start time"""
        for ttype in self.allStat:
            utime = self.allStat[ttype]['starttime']
            utime = datetime.datetime.strptime(utime, "%Y-%m-%d %H:%M:%S").timestamp()
            _taskids[ttype] = utime
#
        for ttype in dict(sorted(_taskids.items(), key=lambda item: item[1])):
            ttypes.append(ttype)
            statList.append(self.allStat[ttype])
        dfs = pd.DataFrame(statList, index=ttypes)
        tableName = "pandaStat"
        indexName = " Workflow Task "
        comment = """ Panda campaign statistics """
        self.make_table(dfs, tableName, indexName, comment)
#        self.make_styled_table(dfs, tableName)

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
            "collType: 2.2i\n",
            "workNames: Not used now\n",
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

