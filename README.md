#ProdStat
###### This repository contains some example scripts to collect info from Butler and from PanDa
###GetButlerStat.py
####Call: GetButlerStat.p -f inpfile.yaml
 The inpfile.yaml has following format:
```
Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml
Jira: PREOPS-707
workflows:
- 20211001T200704Z
- 20211003T045430Z
- 20211003T164354Z
- 20211011T150425Z
- 20211012T170111Z
- 20211012T194814Z
- 20211014T145653Z
- 20211015T003406Z
maxtask: 30
```
Here workflows represent a list of time stamps that are part of workflow name
maxtask is a maximum number of task files used to calculate average cpu/wall time.

####Operation:
The program will scan butler registry to select _metadata files for tasks in 
given dataflow. Those metadata files will be copied one by one into 
/tmp/tempTask.yaml file from which maxRss and CPU time usage will be 
extracted.
The program collects these data for each task type and calculates total CPU usage for
all tasks of the type. At the end total CPU tame used by all dataflows and
maxRss wil be calculadet and resulting table will be created as
/tmp/butlerStat.png file.
Note the timestamp ID is case sensitive, no lowercase t or z, use YYYYMMDDTHHMMSSZ

###GetPanDaStat.py
####Call: GetPanDaStat.p -f inpfile.yaml
The input file format is exactly same as for GetBatlerStat.py program

####Operation:
The program will query PanDa web logs to select information about dataflows,
tasks and jobs. It will produce 2 tables. The first one gives the status of the campaign
production showing each workflow status.
The second table list completed tasks, number of quanta in each, time spent for each job,
total time for all quanta and wall time estimate for each task. This information permit us to estimate rough number of
parallel jobs used for each task, and campaign in whole.


#### DRPIssueUpdate.py
#### Call: python DRPIssueUpdate.py <step_yaml_file> <DRP-to-update|0-to-create-new-issue> <output-of-GetButlerStat-file> <output-of-GetPanDaStat-file>

#### Operation:
The program will parse the output of the GetButlerStat file and the GetPandaStat file and create JIRA format
tables to be inserted into a DRP jira issue as the description.  If the DRP-to-update is included, it will 
overwrite an existing DRP issue of that number (i.e. DRP-18).  If '0' is given for the 2nd argument, it will
create a new JIRA DRP issue with a running integer index, one higher than the highest one in use.
The <step_yaml_file> is the BPS submit yaml file, it must have keywords such as payload, dataQuery, campaign, etc.
The required keywords (grouped into steps, and unique across steps) may be modified in 
the DRPIssueUpdate.py script.
The 'pipetaskInit' and 'mergeExecutionButler' tasks are unique, in that they are added in at the beginning
and end of each step in a panda Run. 
The tasks for a step are defined in the $OBS_LSST_DIR/pipelines/imsim/DRP.yaml file -- that can (should) be
changed to read it from the pipelineYaml string in the <step_yaml_file> (strip off the step info).

The <output-of-GetButlerStat-file> is created by first running GetButlerStat.py with a -f input.yaml file
which gives the YYYYMMDDTHHMMSSZ id of the run, along with the JIRA ticket embedded in the run name (could be left
off if YYMMDD...Z is unique perhaps?

Same for the output-of-GetPanDaStat file.

Currently the hard part is to track down the step_yaml_file and associate it with a YYMMDDTHHMMSSZ bps run id.
None of that is currently automated. 
There are also cases where multiple YYMMDD...Z bps runs are associated with the same step_yaml file for the run
(because the first one didn't finish completely). There may be special step_yaml files to cleanup or different
YYMMDDS..Z runs which all used the same input step yaml but 'extended the run'.

We are not yet properly dealing with these.


