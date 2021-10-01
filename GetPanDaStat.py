""" This is an example program how to read production statistics
 from PanDa database"""
#!/usr/bin/env python
import urllib.request, json
from urllib.request import urlopen
import time
import datetime

""" First lets get all workflows and sort them by user
 creating a dictionary with user and list of 
 user workflows . 
 Select only recent workflows with creation date after 2021-09-15"""
timeT = time.mktime(time.strptime("2021-09-28", "%Y-%m-%d"))
print("select after:",timeT)
with urlopen("http://panda-doma.cern.ch/idds/wfprogress/?json") as workflow:
    wfdata =json.loads(workflow.read().decode())
    print(len(wfdata))
#
    users=[]
    workflows={}
    for i in range(len(wfdata)):
#
        r_name=wfdata[i]['r_name']
        
        user=''
        if r_name.startswith('u'): 
            user=r_name.split('_')[1]
            
        if user != '' and user not in users:
            users.append(user)
    for user in users:
        workflows[user]=[]
        for i in range(len(wfdata)):
            r_name=wfdata[i]['r_name']
            created = wfdata[i]['created_at']
            dateStart = created.split(' ')[0]
            startD=time.mktime(time.strptime(dateStart, "%Y-%m-%d"))
            if user in r_name and startD >= timeT:
                workflows[user].append(wfdata[i])
            else:
                continue
    for user in users:
        print(' ')
        print(user)
        print(workflows[user])

""" Now select one finished workflow from user hchiang2  and get info
 about tasks """
wf = workflows['hchiang2']
for work in wf:
    if work['r_status'] == 'finished':
        urls=work['r_name']
        print(" \n"," Workflow :",urls)
        with urlopen("http://panda-doma.cern.ch/tasks/?taskname="+urls+"*&days=5&json") as wftasks:
            tasks = json.loads(wftasks.read().decode())
            print(len(tasks))
            for task in tasks:
                print(' ')
                print(task)
                taskid=task['jeditaskid']
                urlst="http://panda-doma.cern.ch/job?pandaid="+str(taskid)+"&json"
#                print(urlst)
                with urlopen(urlst) as url:
                    data = json.loads(url.read().decode())

                    print(data.keys())
                    files = data['files']
                    jobs = data['job']
                    dsfiles = data['dsfiles']
                    status = jobs['jobstatus']
                    if status == 'failed':
                        print("__________________________________________________________________")
                        print("pandaid:",jobs['pandaid']," production label:",jobs['prodsourcelabel']," attempt number:",jobs['attemptnr'])
                        print("jobname:",jobs['jobname']," cpu consumption:",jobs['cpuconsumptiontime']," cpu consuption unit:",jobs['cpuconsumptionunit']," disk usage:",jobs['maxdiskcount'],jobs['maxdiskunit'])
                        print(" ram usage:",jobs['minramcount'],jobs['minramunit']," # cores:",jobs['actualcorecount']," status:",jobs['jobstatus'])
                        print(" duration:",jobs['duration']," wait time:",jobs[ 'waittime']," error code",jobs['taskbuffererrorcode'])
                        print("errorinfo:",jobs['errorinfo'])
                    else:
                        print(jobs)
                        print("__________________________________________________________________")
                        print("pandaid:",jobs['pandaid']," production label:",jobs['prodsourcelabel']," attempt number:",jobs['attemptnr'])
                        print("jobname:",jobs['jobname']," cpu consumption:",jobs['cpuconsumptiontime']," cpu consuption unit:",jobs['cpuconsumptionunit']," disk usage:",jobs['maxdiskcount'],jobs['maxdiskunit'])
                        print(" ram usage:",jobs['minramcount'],jobs['minramunit']," # cores:",jobs['actualcorecount']," status:",jobs['jobstatus'])
                        print(" duration:",jobs['duration']," wait time:",jobs[ 'waittime']," error code",jobs['taskbuffererrorcode'])
                        print("errorinfo:",jobs['errorinfo'])
            
#for i in range(len(tasks)):
#    taskid=tasks[i]['jeditaskid']
#    urls="http://panda-doma.cern.ch/job?pandaid="+str(taskid)+"&json"
#    print(urls)
##    with urlopen("http://panda-doma.cern.ch/job?pandaid=1693070&json") as url:
#    with urlopen("http://panda-doma.cern.ch/job?pandaid=5798&json") as url:
#        data = json.loads(url.read().decode())
##        print(data)
#        print(data.keys())
#        files = data['files']
#        jobs = data['job']
#        dsfiles = data['dsfiles']
##        print("files ",files)
#        print(" jobs ",jobs)
#        print(' ')
#        jobkeys= jobs.keys()
##        print("Jobs keys: ",jobkeys)
#        print("__________________________________________________________________")
#        print("pandaid:",jobs['pandaid']," production label:",jobs['prodsourcelabel']," attempt number:",jobs['attemptnr'])
#        print("jobname:",jobs['jobname']," # cpus:",jobs['maxcpucount']," disk usage:",jobs['maxdiskcount'],jobs['maxdiskunit'])
#        print(" ram usage:",jobs['minramcount'],jobs['minramunit']," # cores:",jobs['actualcorecount']," status:",jobs['jobstatus'])
#        print(" duration:",jobs['duration']," wait time:",jobs[ 'waittime'])
#        print("dsfiles",dsfiles)


