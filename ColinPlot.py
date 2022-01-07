import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import json
import datetime
import urllib.request
import glob

matplotlib.rcParams['figure.dpi'] = 140
req_id = 510
tasklist_text = urllib.request.urlopen("http://panda-doma.cern.ch/tasks/?json&reqid={req_id:d}".format(req_id=req_id)).read()
tasklist_json = json.loads(tasklist_text)
task_ids = list([x['jeditaskid'] for x in tasklist_json])
task_names = list([x['taskname'] for x in tasklist_json])

job_url = "http://panda-doma.cern.ch/jobs/?json&jeditaskid={taskid:d}&fields=pandaid,starttime,endtime,durationsec"

for task in tasklist_json:
    task_id = int(task['jeditaskid'])
    file_path = "reqid{:d}_task_{:d}.json".format(req_id, task_id)
    if (os.path.exists(file_path)):
        print("{:s} exists, skipping".format(file_path))
        continue

    task_query = urllib.request.urlopen(job_url.format(taskid=task_id))
    with open(file_path, "wb") as f:
        f.write(task_query.read())
        print("Wrote {:s}".format(file_path))


    def get_job_start_duration_by_filename(filename):

        with open(filename) as f:
            j = json.load(f)

        start_times = list([datetime.datetime.fromisoformat(x['starttime']) for x in j['jobs']])
        duration = list([int(x['durationsec']) for x in j['jobs']])

        return start_times, duration


    def get_job_start_duration(req_id, task_id):
        filename = "reqid{:d}_task_{:d}.json".format(req_id, task_id)
        return get_job_start_duration_by_filename(filename)


    task_short_names = ["isr", "characterizeImage", "calibrate",
                        "writeSourceTable", "transformSourceTable"]
    # Config parameters
    # The scale factor effectively is the bin size in minutes
    scale_factor = 12
    n_bins = 5000
    # We want the tasks to always get plotted in the same order for consistency when comparing between runs.
    # Also allows us to show times relative to pipetaskInit.

    ordered_tasks = []
    for name in task_short_names:
        matches = list(filter(lambda x: name in x['taskname'], tasklist_json))
        assert len(matches) == 1, "Wrong number of task name matches"
        ordered_tasks.append(matches[0])

    init_matches = list(filter(lambda x: "pipetaskInit" in x['taskname'], tasklist_json))
    assert len(init_matches) == 1, "Did not find pipetaskInit"
    pipetaskInit_start_times, _ = get_job_start_duration(req_id, init_matches[0]['jeditaskid'])
    init_time = min(pipetaskInit_start_times)

    for (short_name, task) in zip(task_short_names, ordered_tasks):

        task_id = task['jeditaskid']

        task_count = np.zeros(n_bins)

        job_starts, job_durations = get_job_start_duration(req_id, task_id)

        for start_time, duration in zip(job_starts, job_durations):
            start_time_seconds = (start_time - init_time).seconds
            task_count[int(start_time_seconds / scale_factor):int((start_time_seconds + duration) / scale_factor)] += 1

        plt.plot(np.arange(n_bins) * scale_factor / 3600, task_count, label=short_name)

    plt.xlabel("Hours since first quantum start")
    plt.ylabel("Number of running quanta")
    plt.legend(loc=0, frameon=False)
    plt.title("request id {:d}".format(req_id))
    plt.savefig("reqid_{:d}_timing.png".format(req_id), overwrite=True)

    all_reqids = [510, 511, 512, 513, 514, 515, 516, 517, 519, 520]

    scale_factor = 20
    n_bins = 5000

    task_count_totals = np.zeros(n_bins)

    # Find the earliest start time in the first task
    initial_start_time = None
    for filename in glob.glob("reqid{:d}_*json".format(all_reqids[0])):
        with open(filename) as f:
            j = json.load(f)
        start_times = list([datetime.datetime.fromisoformat(x['starttime']) for x in j['jobs']])
        if (initial_start_time is not None):
            if (initial_start_time > min(start_times)):
                initial_start_time = min(start_times)
        else:
            initial_start_time = min(start_times)

    for reqid in all_reqids:

        task_count = np.zeros(n_bins)

        for filename in glob.glob("reqid{:d}_*json".format(reqid)):

            job_starts, job_durations = get_job_start_duration_by_filename(filename)

            for start_time, duration in zip(job_starts, job_durations):
                start_time_seconds = (start_time - initial_start_time).seconds
                task_count[
                int(start_time_seconds / scale_factor):int((start_time_seconds + duration) / scale_factor)] += 1

        plt.plot(np.arange(n_bins) * scale_factor / 3600, task_count, label="{:d}".format(reqid))

        task_count_totals += task_count

    plt.xlabel("Hours since first quantum start")
    plt.ylabel("Number of running quanta")
    plt.savefig("all_reqid_timing.png".format(req_id), overwrite=True)

    plt.plot(np.arange(n_bins) * scale_factor / 3600, task_count_totals)

    plt.xlabel("Hours since first quantum start")
    plt.ylabel("Number of running quanta")
    plt.savefig("sum_reqid_timing.png".format(req_id), overwrite=True)
