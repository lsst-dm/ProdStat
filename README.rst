########
ProdStat
########

``prodstat`` provides scripts which are used  to organize DP0.2 production and collect production statistics.
Collected statistics in form of plots and tables can be reported to corresponding Jira tickets.

Obtaining the package
=====================
setup lsst_distrib

git clone https://github.com/lsst-dm/ProdStat.git

cd ProdStat

to update
---------
git update

Set up the package
==================
cd ProdStat

setup ProdStat -r .

Running tests
-------------
scons

Using the package
-----------------
prodstat --help

Usage: prodstat [OPTIONS] COMMAND [ARGS]...

  Command line interface for ProdStat.

Options:
  --help  Show this message and exit.

Commands:

-  `add-job-to-summary`:  Add a summary to a job summary table.
-  `get-butler-stat` :    Build production statistics tables using Butler...
-  `get-panda-stat` :     Build production statistics tables using PanDa...
-  `init` :               Initialize a DRP issue.
-  `make-prod-groups` :    Split a list of exposures into groups defined in yaml...
-  `plot-data` :          Create timing data of the campaign jobs.
-  `prep-timing-data` :    Create timing data of the campaign jobs Parameters...
-  `report-to-jira` :     Report production statistics to a Jira ticket...
-  `update-issue` :       Update or create a DRP issue.
-  `update-stat` :        Update issue statistics.

Obtaining help on command
"""""""""""""""""""""""""
prodstat COMMAND --help


Organizing production
=====================

  setup lsst_distrib
  mkdir mywork
  cd mywork
  git clone https://github.com/lsst-dm/ProdStat.git
  cd ProdStat
  setup ProdStat -r .
  cd ../

it is also useful to have the dp02-processing package which has the
DC0.2 explist and some sample template bps submit scripts and
auxillary bps includes like memoryRequest.yaml and clustering.yaml::

  git clone https://github.com/lsst-dm/dp02-processing.git

and to update::

  cd dp02-processing; git update
The explist, templates, and clustering yaml memoryRequest yaml are in: dp02-processing/full/rehearsal/PREOPS-938/

On your data-int.lsst.cloud note, to enable running scripts, like update-issue, etc \
one needs to install jira locally in you home area and add a login credential .netrc file.
To install jira to this::

  pip install jira

Until tokens are enabled for jira access, one can use a .netrc file for jira authentication.


submit a job to bps, record it in an issue
------------------------------------------

Do this::

  bps submit clusttest-all-1.yaml

  prodstat issue-update clusttest-all-1.yaml PREOPS-XXX DRP0 [20211225T122512Z]


or::

  prodstat issue-update clusttest-all-1.yaml PREOPS-XXX

(and it will pick the most recent timestamp that it can find with that PREOPS-XXX in the submit tree)

(this will return a new DRP-YYY issue number, recall it)
prodstat add-job-to-summary PREOPS-XXX DRP-YYY
(then look at DRP-55 or DRP-53 for the current table.


You can remove an unwanted entry from the DRP-55 table by doing this::

  prodstat add-job-to-summary PREOPS-XXX DRP-YYY remove

Update Butler, Panda Stats when job is done
-------------------------------------------

When job completes, you can update the stats table in the DRP-YYY ticket with this call::

  prodstat update-stat  PREOPS_XXX DRP-YYY


this will take several minute to query the butler, panda and generate the updated stats)
Then::

  prodstat add-job-to-summary PREOPS-XXX DRP-YYY

(this will then update the entry in the DRP-55 table with the new nTasks,nFiles,nFinished,nFail,nSub
stats)

Commands
========

init
----
prodstat init [OPTIONS] PBS_SUBMIT_TEMPLATE PRODUCTION_ISSUE [DRP_ISSUE]

Initialize a DRP issue.
Parameters
""""""""""
 bps_submit_template : `str`
    Template file with place holders for start/end
    dataset/visit/tracts (will be attached to Production Issue
 production_issue : `str`
    Pre-existing issue of form PREOPS-XXX (later DRP-XXX) to update
    with link to ProdStat tracking issue(s) -- should match issue
    in template keyword.
 drp_issue : `str`
    If present in form DRP-XXX, redo by overwriting an
    existing DRP-issue. If not present, create a new DRP-issue.
    All ProdStat plots and links for group of bps submits will be
    tracked off this DRP-issue.  Production Issue will be updated with
    a link to this issue, by updating description (or later by using
    subtask link if all are DRP type).

issue-update
------------

 prodstat update-issue [OPTIONS] BPS_SUBMIT_FNAME PRODUCTION_ISSUE [DRP_ISSUE]
   Update or create a DRP issue.
   Parameters
   """"""""""
   bps_submit_fname : `str`
     The file name for the BPS submit file (yaml).
     Should be sitting in the same dir that bps submit was done,
     so that the submit/ dir can be searched for more info
   production_issue : `str`
     PREOPS-938 or similar production issue for this group of
     bps submissions
   drp_issue : `str`
     DRP issue created to track ProdStat for this bps submit
   ts : `str`
     time stamp

Options:
""""""""
  --ts TEXT
  --help     Show this message and exit.
Example:
""""""""
  prodstat issue-update ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938 DRP0 [20211225T122522Z]

or:

  prodstat issue-update ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938
(this will use the latest timestamp in the submit subdir)

This will return a new DRP-XXX issue where the  prodstats for the PREOPS-938 issue step will be stored
and updated later.


make-prod-groups
----------------
  prodstat make-prod-groups [OPTIONS] TEMPLATE [all|f|u|g|r|i|z|y] GROUPSIZE SKIPGROUPS NGROUPS EXPLIST
    Split a list of exposures into groups defined in yaml files.

Parameters
""""""""""
  template : `str`
    Template file with place holders for start/end dataset/visit/tracts
        (optional .yaml suffix here will be added)
    band : `str`
        Which band to restrict to (or 'all' for no restriction, matches BAND
        in template if not 'all')
    groupsize : `int`
      How many visits (later tracts) per group (i.e. 500)
    skipgroups: `int`
      skip <skipgroups> groups (if others generating similar campaigns)
    ngroups : `int`
      how many groups (maximum)
    explists : `str`
      text file listing <band1> <exposure1> for all visits to use


add-job-to-summary
------------------

  prodstat add-job-to-summary DRP-XX PREOPS-YY [reset|remove]

   DRP-XX is the issue created to track ProdStat for this bps submit.
   If you run the command twice with the same entries, it is ok.
   If you specify remove, it will instead remove one entry from the table with the DRP/PREOPS number.
   If you specify reset is will erase the whole table (don't do this lightly).

To see the output summary: View special DRP tickets DRP-53 (all bps submits entered) and https://jira.lsstcorp.org/browse/DRP-55 (step1 submits only)


get-butler-stat
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
 - `collType` is a second token used to uniquely identify workflow, it can be part of workflow time stamp or user name, etc.
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

  MakePandaPlots.py prep-timing-data ./inp_file.yaml
  
The input yaml file should contain following parameters:

 - `Jira: PREOPS-905` - jira ticket corresponding given campaign.
 - `collType: 2.2i` - a token to help identify campaign workflows.
 - `bin_width: 30.` - the width of the plot bin in sec.
 - `n_bins: 100000` - total number of bins in plots
 - `job_names` - a list of job names
   - `'pipeTaskInit'`
   - `'mergeExecutionButler'`
   - `'visit_step2'`
 - `start_at: 0.` - plot starts at hours from first quanta
 - `stor_at: 10.` - plot stops at hours from first quanta

The program scan panda database to collect timing information for all job types in the list.
It creates then timing information in /tmp directory with file names like::

  panda_time_series_<job_type>.csv

Making Plots
------------

Call::
  
  MakePandaPlots.py plot-data inp_file.yaml

The program reads timing data created by prep-timing-data command and
build plots for each type of jobs in given time boundaries.
each type of jobs in given time boundaries.



