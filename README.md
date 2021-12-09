#ProdStat
###### This repository contains some example scripts to collect info from Butler and from PanDa
###GetButlerStat.py
####Call: GetButlerStat.p -f inpfile.yaml
 The inpfile.yaml has following format:
```
Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml
Jira: PREOPS-707
collType:2.2i
workflows: 
maxtask: 30
```
Here  Jira: represent Jira ticket that is used to identify workflows (data collections ) ,
collType: is a second token used to uniquely identify workflow, it can be part of workflow time stamp
or user name, e.t.c.
workflows: is not used now,
maxtask: maximum number of tasks (yaml) files to process for average cpu/wall time estimation.

####Operation:
The program will scan butler registry to select _metadata files for tasks in 
given workflow. Those metadata files will be copied one by one into 
/tmp/tempTask.yaml file from which maxRss and CPU time usage will be 
extracted.
The program collects these data for each task type and calculates total CPU usage for
all tasks of the type. At the end total CPU time used by all workflows and
maxRss wil be calculated and resulting table will be created as
/tmp/butlerStat-PREOPS-707.png file. The text version of the table used to put in Jira comment is also created
as /tmp/butlerStat-PREOPS-707.txt

###GetPanDaStat.py
####Call: GetPanDaStat.p -f inpfile.yaml
The input file format is exactly same as for GetButlerStat.py program

####Operation:
The program will query PanDa web logs to select information about workflows,
tasks and jobs. It will produce 2 sorts of tables.

The first one gives the status of the campaign
production showing each workflow status as /tmp/pandaWfStat-PREOPS-707.txt.
A styled html table also is created as /tmp/pandaWfStat-PREOPS-707.html

The second table type lists completed tasks, number of quanta in each, time spent for each job,
total time for all quanta and wall time estimate for each task. This information permit us to estimate rough number of
parallel jobs used for each task, and campaign in whole.
The table names created as /tmp/pandaStat-PREOPS-728.png and pandaStat-PREOPS-728.txt.

Hear PREOPS-XXX tokens represent Jira ticket the statistics is collected for.


#### DRPInit.py 
Usage: DRPInit.py <bps_submit_yaml_template> <Production Issue> [DRP-issue]
  <bps_submit_yaml_template>: Template file with place holders for start/end dataset/visit/tracts (will be attached to Production Issue)
  <Production Issue>: Pre-existing issue of form PREOPS-XXX (later DRP-XXX) to update with link to ProdStat tracking issue(s) -- should match issue in template keyword 
  [DRP-issue]: If present in form DRP-XXX, redo by overwriting an existing DRP-issue. If not present, create a new DRP-issue.  All ProdStat plots and links for group of bps submits will be tracked off this DRP-issue.  Production Issue will be updated with a link to this issue, by updating description (or later by using subtask link if all are DRP type). 

Example: 
git clone https://github.com/lsst-dm/ProdStat.git
git clone https://github.com/lsst-dm/dp02-processing.git
setup lsst_distrib
export PYTHONPATH=${PYTHONPATH}:<home/yourname/ProdStat>
export PATH=${PATH}:</home/yourname/ProdStat>

mkdir mywork
cd mywork
DRPInit.py ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938

This will return a new DRP-XXX issue where the  prodstats for the PREOPS-938 issue step will be stored
and updated later.


############## MakeProdGroups.py 
Usage: MakeProdGroups.py <bps_submit_yaml_template> <band|'all'> <groupsize(visits/group)> <skipgroups(skip first skipgroups groups)> <ngroups> <explist>
  <bps_submit_yaml_template>: Template file with place holders for start/end dataset/visit/tracts (optional .yaml suffix here will be added)
 <band|'all'> Which band to restrict to (or 'all' for no restriction, matches BAND in template if not 'all')
 <groupsize> How many visits (later tracts) per group (i.e. 500)
 <skipgroups> skip <skipgroups> groups (if others generating similar campaigns
 <ngroups> how many groups (maximum)
 <explist> text file listing <band1> <exposure1> for all visits to use

Example: (same setup as for DRPInit.py)
mkdir mywork
cd mywork
MakeProdGroups.py ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml  all 500 0 100 ../dp02-processing/full/rehearsal/PREOPS-938/explist

 

##################### submit a job to bps, record it in an issue

bps submit clusttest-all-1.yaml

(this is not yet implemented, but will add to the DRP- ticket created in DRPInit.py)
DRPIssueUpdate.py clusttest-all-1.yaml 
