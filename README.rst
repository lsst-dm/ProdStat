########
ProdStat
########

This repository contains some example scripts to collect info from Butler and from PanDa

GetButlerStat.py
----------------

Call::

  GetButlerStat.p -f inpfile.yaml

The inpfile.yaml has following format::

  Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml
  Jira: PREOPS-707
  collType:2.2i
  workflows: 
  maxtask: 30

  
Here,

 - `Jira` represents Jira ticket that is used to identify workflows (data collections ) ,\
 - `collType` is a second token used to uniquely identify workflow, it can be part of workflow time stamp
or user name, etc.
 - `workflows` is not used now,
 - `maxtask` maximum number of tasks (yaml) files to process for average cpu/wall time estimation.


This program will scan butler registry to select _metadata files for
tasks in given workflow. Those metadata files will be copied one by
one into /tmp/tempTask.yaml file from which maxRss and CPU time usage
will be extracted.  The program collects these data for each task type
and calculates total CPU usage for all tasks of the type. At the end
total CPU time used by all workflows and maxRss wil be calculated and
resulting table will be created as /tmp/butlerStat-PREOPS-XXX.png
file. The text version of the table used to put in Jira comment is
also created as /tmp/butlerStat-PREOPS-XXX.txt

GetPanDaStat.py
--------------

Call::

  GetPanDaStat.p -f inpfile.yaml
  
The input file format is exactly same as for GetButlerStat.py program

The program will query PanDa web logs to select information about workflows,
tasks and jobs. It will produce 2 sorts of tables.

The first one gives the status of the campaign production showing each
workflow status as /tmp/pandaWfStat-PREOPS-XXX.txt.  A styled html
table also is created as /tmp/pandaWfStat-PREOPS-XXX.html

The second table type lists completed tasks, number of quanta in each,
time spent for each job, total time for all quanta and wall time
estimate for each task. This information permit us to estimate rough
number of parallel jobs used for each task, and campaign in whole.
The table names created as /tmp/pandaStat-PREOPS-XXX.png and
pandaStat-PREOPS-XXX.txt.

Hear PREOPS-XXX tokens represent Jira ticket the statistics is collected for.

MakePandaPlots.py
-----------------

Call::

  MakePandaPlots.py make-panda-plots ./inpfile.yaml
  
The input yaml file should contain following parameters:

 - `Jira: PREOPS-905` - jira ticket corresponding given campaign.
 - `collType: 2.2i` - a token to help identify campaign workflows.
 - `bin_width: 30.` - the width of the plot bin in sec.
 - `n_bins: 100000` - total number of bins in plots

The program scan panda database to collect timing information for all job types in the campaign.
It creates then plots of jobs time distribution for each job type.
The names of plots create are like::

  timing_<job_type>.png

The program also saves timing information in /tmp directory with file names like::

  panda_time_series_<job_type>.csv 

MakePlots.py
------------

Call::
  
  MakePlots.py make-plots plot.yaml
  
The input yaml file should contain following parameters::
  
 - `bin_width: 30.` - width of the plot bin in sec.
 - `start_at: 0.` - time shift the plot begins with in hours.
 - `stop_at: 550.` - time shift the plot ends in hours. 

The program reads timing data created by MakePandaPlots.py and build plots for
each type of jobs in given time boundaries.

**Caution**

The list of known job types is hardcoded in MakePandaPlots.py and
MakePlots.py. In future development we will transfer this information
in the input file.

DRPIssueUpdate.py
-----------------

Usage::
  
  DRPIssueUpdate.py <bps_submit_yaml_template> <Production Issue> [DRP-issue|DRP0] [timedatestampid]
  
 - `bps_submit_yaml_template`: Template file with place holders for start/end dataset/visit/tracts (will be attached to Production Issue) 
 - `Production Issue`: Pre-existing issue of form PREOPS-XXX (later DRP-XXX) to update with link to ProdStat tracking issue(s) -- should match issue in template keyword
 - `[DRP-issue|DRP0]`: If present in form DRP-XXX, redo by overwriting an existing DRP-issue. If not present or DRP0: create a new DRP-issue.  All ProdStat plots and links for group of bps submits will be tracked off this DRP-issue.
 - `[timedatestampid]`: by default DRPIssueUpdate looks for a timestampid subdir in the submit directory tree with the most recent stamp. If you are 'redoing' this, then include the DRP-XXX issue to overwrite *and* include the correct timedatestampid.

Example::
  
  git clone https://github.com/lsst-dm/ProdStat.git
  git clone https://github.com/lsst-dm/dp02-processing.git
  setup lsst_distrib
  export PYTHONPATH=${PYTHONPATH}:<home/yourname/ProdStat>
  export PATH=${PATH}:</home/yourname/ProdStat>

  mkdir mywork
  cd mywork
  DRPIssueUpdate.py ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938 DRP0 [20211225T122522Z]

or::
  
  DRPIssueUpdate.py ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938 \

(this will use the latest timestamp in the submit subdir)

This will return a new DRP-XXX issue where the  prodstats for the PREOPS-938 issue step will be stored
and updated later.


MakeProdGroups.py
-----------------

Usage::
  
  MakeProdGroups.py <bps_submit_yaml_template> <band|'all'> <groupsize(visits/group)> <skipgroups(skip first skipgroups groups)> <ngroups> <explist>

 
 - `bps_submit_yaml_template`: Template file with place holders for start/end dataset/visit/tracts (optional .yaml suffix here will be added)
 - `band|'all`: Which band to restrict to (or 'all' for no restriction, matches BAND in template if not 'all')
 - `groupsize`: How many visits (later tracts) per group (i.e. 500)
 - `skipgroups`: skip <skipgroups> groups (if others generating similar campaigns
 - `ngroups`: how many groups (maximum)
 - `explist`: text file listing <band1> <exposure1> for all visits to use

Example (same setup as for DRPIssueUpdate.py)::

  mkdir mywork
  cd mywork
  MakeProdGroups.py ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml  all 500 0 100 ../dp02-processing/full/rehearsal/PREOPS-938/explist

DRPAddJobToSummary.py
---------------------

usage::
  
  DRPAddJobToSummary.py DRP-XX PREOPS-YY [reset|remove]

DRP-XX is the issue created to track ProdStat for this bps submit.

If you run the command twice with the same entries, it is ok.

If you specify remove, it will instead remove one entry from the table with the DRP/PREOPS number.

If you specify reset is will erase the whole table (don't do this lightly).

To see the output summary: View special DRP tickets DRP-53 (all bps submits entered) and https://jira.lsstcorp.org/browse/DRP-55 (step1 submits only)

submit a job to bps, record it in an issue
------------------------------------------

Do this::

  bps submit clusttest-all-1.yaml

  DRPIssueUpdate.py clusttest-all-1.yaml PREOPS-XXX DRP0 [20211225T122512Z]

  
or::

  DRPIssueUpdate.py clusttest-all-1.yaml PREOPS-XXX

(and it will pick the most recent timestamp that it can find with that PREOPS-XXX in the submit tree)

(this will return a new DRP-YYY issue number, recall it)
DRPAddToSummary PREOPS-XXX DRP-YYY
(then look at DRP-55 or DRP-53 for the current table.


You can remove an unwanted entry from the DRP-55 table by doing this::
  
  DRPAddToSummary PREOPS-XXX DRP-YYY remove

Update Butler, Panda Stats when job is done
-------------------------------------------

When job completes, you can update the stats table in the DRP-YYY ticket with this call::

  DRPStatUpdate.py PREOPS_XXX DRP-YYY

  
this will take several minute to query the butler, panda and generate the updated stats)
Then::

  DRPAddToSummary PREOPS-XXX DRP-YYY

(this will then update the entry in the DRP-55 table with the new nTasks,nFiles,nFinished,nFail,nSub 
stats)

initial setup for JIRA and ProdStat (before its in the production stack)
------------------------------------------------------------------------

On your data-int.lsst.cloud note, to enable running scripts, like DRPIssueUpdate.py, etc \
one needs to install jira locally in you home area and add a login credential .netrc file.
To install jira to this::

  pip install jira

Until tokens are enabled for jira access, one can use a .netrc file.

To call the ProdStat routines, such as MakeProdGroups and
DRPIssueUpdate.py you will need to check out the packages from git::

  cd
  git clone https://github.com/lsst-dm/ProdStat

to update::

  cd  ProdStat; git update)

it is also useful to have the dp02-processing package which has the
DC0.2 explist and some sample template bps submit scripts and
auxillary bps includes like memoryRequest.yaml and clustering.yaml::

  cd
  git clone https://github.com/lsst-dm/dp02-processing

and to update::
  
  cd dp02-processing; git update

The explist, templates, and clustering yaml memoryRequest yaml are in: dp02-processing/full/rehearsal/PREOPS-938/


