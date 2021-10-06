#!/usr/bin/env python
""" This is an example program how to read production statistics
 from PanDa database"""

import sys
import json
from urllib.request import urlopen
import time
import getopt


class GetPanDaStat:
    def __init__(self, times, username):
        """Set the time threshold in form 2021-09-28. Will be used for selection from time to now"""
        print(time.strptime(times, "%Y-%m-%d"))
        self.timeT = time.mktime(time.strptime(str(times), "%Y-%m-%d"))
        self.users = []
        self.workflows = {}
        self.userWorks = []
        self.user = username
        self.flowStat = {}

    def GetWorkflows(self):
        """ First lets get all workflows and sort them by user
        creating a dictionary with user and list of
        user workflows .
        Select only recent workflows with creation date a"""
        with urlopen("http://panda-doma.cern.ch/idds/wfprogress/?json") as workflow:
            wfdata =json.loads(workflow.read().decode())
            print(len(wfdata))
#
        for i in range(len(wfdata)):
#
            r_name=wfdata[i]['r_name']
            _user=''
            if r_name.startswith('u'):
                _user=r_name.split('_')[1]
                if _user != '' and _user not in self.users:
                    self.users.append(_user)
        for _user in self.users:
            self.workflows[_user]=[]
            for i in range(len(wfdata)):
                r_name=wfdata[i]['r_name']
                created = wfdata[i]['created_at']
                dateStart = created.split(' ')[0]
                startD=time.mktime(time.strptime(dateStart, "%Y-%m-%d"))
                if _user in r_name and startD >= self.timeT:
                    self.workflows[_user].append(wfdata[i])
                else:
                    continue
        for _user in self.users:
            print(' ')
            print(_user)
            print(self.workflows[_user])


    """ Select given workflow from list of user workflows"""
    def GetUserWork(self,user,workflow):
        work = {}
        wf = self.workflows[user]
        for work in self.workflows[user]:
            if work['r_name'] == workflow:
                return work
            else:
                return work

    " Select tasks for given workflow"
    def GetWfTasks(self,workflow):
        tasks=[]
        urls = workflow['r_name']
        with urlopen("http://panda-doma.cern.ch/tasks/?taskname=" + urls + "*&days=5&json") as wftasks:
            tasks = json.loads(wftasks.read().decode())
        return tasks

    " extract data we need from task dictionary "
    def GetTaskInfo(self,task,result):
        data={}
#        print(task)
#        print('..... ')
#        print(result.keys())
        files = result['files'][0]
#        print(files)
        job = result['job']
#        print(job)
        dsfiles = result['dsfiles']
#        print(dsfiles)
        data['jeditaskid'] = task['jeditaskid']
        data['jobname'] = job['jobname']
        data['taskname'] = task['taskname']
        data['status'] = task['status']
        data['attemptnr'] = files['attemptnr']
        data['corecount'] = task['corecount']
        data['maxattempt'] = job['maxattempt']
        data['basewalltime'] = task['basewalltime']
        data['cpuefficiency'] = task['cpuefficiency']
        data['maxdiskcount'] = job['maxdiskcount']
        data['maxdiskunit'] = job['maxdiskunit']
        data['cpuconsumptiontime'] = job['cpuconsumptiontime']
        data['jobstatus'] = job['jobstatus']
        data['duration'] = job['durationsec']
#        print(data)
        return data

    " given list of tasks get statistics for each task type "
    def GetTaskData(self,tasks):
        " First create dictionary of task:data "
        taskData =[]
        taskNames=[]
        taskTypes = {}
        for task in tasks:
            taskid = task['jeditaskid']
            urlst = "http://panda-doma.cern.ch/job?pandaid=" + str(taskid) + "&json"
            data = {}  # dictionary for selected data
            with urlopen(urlst) as url:
                result = json.loads(url.read().decode())
                data = self.GetTaskInfo(task,result)
                taskData.append(data)
        " Now create a list of task types"
        for data in taskData:
            name = (data['jobname']).split('Task')[0]
            if name not in taskNames:
                taskNames.append(name)
        " Now create a list of tasks for each task type "
        for taskname in taskNames:
            tasklist=[]
            for task in taskData:
                if taskname in task['jobname']:
                    tasklist.append(task)
            taskTypes[taskname] = tasklist
        return taskTypes




    def GetTasks(self,user):
        """ Now select one finished workflow from user like hchiang2  and get info
        about tasks """
        wf = self.workflows[user]
        for work in wf:
            if work['r_status'] == 'finished':
                urls=work['r_name']
                print(" \n"," Workflow :",urls)
                tasks = self.GetWfTasks(work)
                taskTypes=self.GetTaskData(tasks)
                " Now calculate statistics for each workflow "
                self.GetStat(taskTypes)

    def GetStat(self,taskTypes):
        wfWall = 0
        wfDisk = 0
        wfCPU = 0
        for ttype in taskTypes:
            tasks = taskTypes[ttype]
            ntasks = len(tasks)
            cpuconsumption = 0
            walltime = 0
            cpuefficiency = 0
            corecount = 0
            maxdiskcount = 0
            duration = 0.
            taskgood = 0
            taskbad = 0
            nfailed = 0
            maxdiskunit = 'MB'
            for i in range(len(tasks)):
                walltime += int(tasks[i]['basewalltime'])
                cpuconsumption += int(tasks[i]['cpuconsumptiontime'])
                cpuefficiency += int(tasks[i]['cpuefficiency'])
                maxdiskcount += int(tasks[i]['maxdiskcount'])
                duration += float(tasks[i]['duration'])
                corecount += int(tasks[i]['corecount'])
                if str(tasks[i]['jobstatus']) == 'finished':
                    taskgood += 1
                else:
                    taskbad += 1
                if int(tasks[i]['maxattempt']) > 1:
                    nfailed += (int(tasks[i]['maxattempt']) -1)
                maxdiskunit = tasks[i]['maxdiskunit']
            wfWall += walltime
            wfDisk += maxdiskcount
            wfCPU += corecount
            print(ttype," total walltime=",walltime,' per job=',walltime/ntasks)
            print('       ','cpuconsumption=',cpuconsumption,' per job=',cpuconsumption/ntasks)
            print('       ',' cpuefficiency=',cpuefficiency/ntasks)
            print('       ','maxdiskcount=',maxdiskcount/ntasks,' ',maxdiskunit)
            print('       ','duration=',duration/ntasks,' sec')
            print('       # failed attempts=',nfailed,' # complete =',taskgood,' # failed =',taskbad)
        print('Workflow walltime sum=',wfWall,' wfDisk usage =',wfDisk,' wfCPU units =',wfCPU)

    " "
    def run(self):
        user = self.user
        self.GetWorkflows()
        self.GetTasks(user)


if __name__ == "__main__":
    print(sys.argv)
    nbpar = len(sys.argv)
    if nbpar < 2:
        print("Usage: GetPanDaStat.py <required inputs>")
        print("  Required inputs:")
        print("  -t <timeS> - start date in format YY-mm-dd")
        print("  -u <username> - user to get data for")
        sys.exit(-2)

    try:
        opts, args = getopt.getopt(sys.argv[1:],"ht:u:",["timeS=","username="])
    except getopt.GetoptError:
        print("Usage: GetPanDaStat.py <required inputs>")
        print("  Required inputs:")
        print("  -t <timeS> - start date in format YY-mm-dd")
        print("  -u <username> - user to get data for")
        sys.exit(2)
    time_flag = 0
    user_flag = 0
    timeS = ''
    username = ''
    for opt,arg in opts:
        print ("%s %s"%(opt,arg))
        if opt == "-h":
            print("Usage: GetPanDaStat.py <required inputs>")
            print("  Required inputs:")
            print("  -t <timeS> - start date in format YY-mm-dd")
            print("  -u <username> - user to get data for")
            sys.exit(2)
        elif opt in ("-t","--timeS"):
            time_flag = 1
            timeS = arg
        elif opt in ("-u","--username"):
            user_flag = 1
            username = arg

    inpsum = user_flag + time_flag
    if inpsum != 2:
        print("Usage: GetPanDaStat.py <required inputs>")
        print("  Required inputs:")
        print("  -t <timeS> - start date in format YY-mm-dd")
        print("  -u <username> - user to get data for")
        sys.exit(-2)
    # Create new threads
    GPS = GetPanDaStat(timeS,username)

    GPS.run()


    print( "End with GetPanDaStat.py")

