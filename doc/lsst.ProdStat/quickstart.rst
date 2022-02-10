########
ProdStat
########

``prodstat`` provides scripts which are used  to organize DP0.2 production and collect production statistics.
Collected statistics in form of plots and tables can be reported to corresponding Jira tickets.

Obtaining the package -- initial setup
======================================

::

   setup lsst_distrib
   git clone https://github.com/lsst-dm/ProdStat.git
   cd ProdStat
   setup ProdStat -r .
   scons  

Notes on Compiling the package the first time and Running tests
---------------------------------------------------------------

::

   scons

This makes a link between bin.src and the bin dir and compiles anything that needs
compiling (in this case its pure python so no compiling), it also makes sure the python 
is setup in the right place this only needs to be run once after checking out or updating

Not all tests may pass, but proceed for now.

After this initial 'scons', for subsequent runs of prodstat commands,
it is sufficient to run::

  setup lsst_distrib
  cd ProdStat
  setup ProdStat -r .

in ProdStat directory
If you haven't changed any binaries or added any new python files, you don't
have to run scons again.

Set up the package -- subsequent setups
=======================================

::
   
   setup lsst_distrib ;# if you have not done so aready
   cd ProdStat  ;#if you are not there already
   setup ProdStat -r .   

If you with to avoid `cd ProdStat`, you can also do `setup ProdStat -r <mypathtoProdStat>`
where it will find the `ups/ProdStat.table` file to complete the EUPS setup of the product.

Using the package
-----------------

Get help on the command line interface for ProdStat::
   
   prodstat --help
   Usage: prodstat [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:

 :`add-job-to-summary`: Add a summary to a job summary table.
 :`get-butler-stat`:    Build production statistics tables using Butler.
 :`get-panda-stat`:     Build production statistics tables using PanDa.
 :`make-prod-groups`:   Split a list of exposures into groups defined in yaml.
 :`plot-data`:          Create timing data of the campaign jobs.
 :`prep-timing-data`:   Create timing data of the campaign jobs Parameters.
 :`report-to-jira`:     Report production statistics to a Jira ticket.
 :`update-issue`:       Update or create a DRP issue.
 :`update-stat`:        Update issue statistics.

Obtaining help on command
"""""""""""""""""""""""""

::

   prodstat COMMAND --help


Organizing production
=====================

::

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


The tract list, explist, templates, and clustering yaml memoryRequest yaml are in: 
dp02-processing/full/rehearsal/PREOPS-938/

On your data-int.lsst.cloud note, to enable running scripts, like update-issue, etc \
one needs to install jira locally in you home area and add a login credential .netrc file.
To install jira to this::

  pip install jira

Until tokens are enabled for jira access, one can use a .netrc file for jira authentication.
Please ask for help if you need it here for jira authentication.

submit a job to bps, record it in an issue
------------------------------------------

Do this::

  bps submit clusttest-all-1.yaml
  prodstat issue-update clusttest-all-1.yaml PREOPS-XXX

(this will return a new DRP-YYY issue number -- make a note of the DRP-YYY number issued)

(and it will pick the most recent timestamp that it can find with that PREOPS-XXX in your 
submit dir tree)

or::

  prodstat issue-update clusttest-all-1.yaml PREOPS-XXX DRP0 [--ts 20211225T122512Z]

The --ts TIMESTAMP option allows one to create new DRP-YYY issues for a bps submit yaml
long after the initial bps submit is done.  One should search through the submit/ directory
tree to find a directory with the timestamp TIMESTAMP that contains a copy the clusttest-all-1.yaml
submit file to make sure these are in sync.  One may also find the timestamps on the wfprogress
panDa workflow status page.

prodstat add-job-to-summary PREOPS-XXX DRP-YYY
then look at DRP-53 for the current table of tracked completed and running and submitted issues.
DRP-53 is currently a 'magic' issue containing a listing of campaign production runs.

You can remove an unwanted entry from the DRP-53 table by doing this::

  prodstat add-job-to-summary PREOPS-XXX DRP-YYY --remove True

This does not delete the DRP-YYY issue, just removes it from the  DRP-53 summary table listing.
It can be added back in with another prodstat add-job-to-summary command.
This is useful if you get the PREOPS-XXX or DRP-YYY wrong accidently, or wish to remove
test DRP-YYY issues.

Update Butler, Panda Stats when job is partially complete and again when done

When job completes, or while still running (panDa workflow shows it in a 'transforming' state),
you can update the stats table in the DRP-YYY ticket with this call::

  prodstat update-stat PREOPS_XXX DRP-YYY

this will take several minute to query the butler, panda and generate the updated stats

Then::

  prodstat add-job-to-summary PREOPS-XXX DRP-YYY

this will then update the entry in the DRP-53 table with the new nTasks,nFiles,nFinished,nFail,nSub
stats

Commands
========

issue-update
------------

Update or create a DRP issue::
   
   prodstat update-issue BPS_SUBMIT_FNAME PRODUCTION_ISSUE [DRP_ISSUE] [--ts TIMESTAMP]


Parameters
""""""""""

   bps_submit_fname : `str`
     The file name for the BPS submit file (yaml).
     Should be sitting in the same dir that bps submit was done,
     so that the submit/ dir tree can be searched for more info
   production_issue : `str`
     PREOPS-938 or similar production issue for this group of
     bps submissions
   drp_issue : `str`
     DRP-YYY issue created to track ProdStat for this bps submit
     if this is left off or is the special string DRP0, then a 
     new issue will be created and assigned (use this newly created number
     for future prodstat update-stat and prodstat add-job-to-summary calls.
   --ts : `str`
     time stamp of the form YYYYMMDDTHHMMSSZ (i.e. 20220107T122421Z)

Options
"""""""

--ts TEXT  timestamp

--help     Show this message and exit.

Example:
""""""""
  prodstat issue-update ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938 DRP0 --ts 20211225T122522Z

or::

  prodstat issue-update ../dp02-processing/full/rehearsal/PREOPS-938/clusttest.yaml PREOPS-938

this will use the latest timestamp in the submit subdir, and so if you've done any bps submits since 
this one, you should hunt down the correct --ts TIMESTAMP

This will return a new DRP-YYY issue where the  prodstats for the PREOPS-938 issue step will be stored
and updated later.


make-prod-groups
----------------

Split a list of exposures into groups defined in yaml files::

  prodstat make-prod-groups [OPTIONS] TEMPLATE [all|f|u|g|r|i|z|y] GROUPSIZE SKIPGROUPS NGROUPS EXPLIST
    

Parameters
""""""""""

  template : `str`
    Template file with place holders for start/end dataset/visit/tracts
    If these variables are present in a template file:
    GNUM (group number 1--N for splitting a set of visits/tracts),
    LOWEXP (first exposure or tract number in a range)
    HIGHEXP (last exposure or tract number in a range)
    They will be substituted for with the values drawn from the explist/tractlist file
    (an optional .yaml suffix here will be added to each generated bps submit yaml in the group)
  band : `str`
        Which band to restrict to (or 'all' for no restriction, matches BAND
        in template if not 'all'). Currently all is always used instead of
        separating by band
  groupsize : `int`
      How many visits (later tracts) per group (i.e. 500)
  skipgroups: `int`
      skip <skipgroups> groups (if others generating similar campaigns)
  ngroups : `int`
      how many groups (maximum)
  explists : `str`
      text file listing <band1> <exposure1> for all visits to use
      this may alternatively be a file listing tracts instead of exposures/visits.
      valid bands are: ugrizy for exposures/visits and all for tracts (or if the
      band is not needed to be known)

add-job-to-summary
------------------

To add a job to the summary jira tickets::

    prodstat add-job-to-summary DRP-XXX PREOPS-YYY [--remove True]

DRP-XX is the issue created to track ProdStat for this bps submit.

If you run the command twice with the same entries, it is ok.

If you specify --remove True, it will instead remove one entry from the table with the DRP/PREOPS number.

To see the output summary: View special DRP tickets DRP-53 (all bps submits entered) and https://jira.lsstcorp.org/browse/DRP-55 (step1 submits only)


get-butler-stat
----------------

Call::

  prodstat get-butler-stat inpfile.yaml

After the task is finished the information in butler metadata will be scanned and corresponding tables will
be created in /tmp/ directory.

The inpfile.yaml has following format::

  Butler: s3://butler-us-central1-panda-dev/dc2/butler.yaml ; or butler-external.yaml on LSST science platform
  Jira: PREOPS-905 ; jira ticket information for which will be selected
  collType: 2.2i ; a token which help to uniquely recognize required data collection
  maxtask: 30 ; maximum number of tasks to be analyzed to speed up the process
  start_date: '2022-01-30' ; dates to select data, which will help to skip previous production steps
  stop_date: '2022-02-02'
  

This program will scan butler registry to select _metadata files for
tasks in given workflow. Those metadata files will be copied one by
one into /tmp/tempTask.yaml file from which maxRss and CPU time usage
will be extracted.  The program collects these data for each task type
and calculates total CPU usage for all tasks of the type. At the end
total CPU time used by all workflows and maxRss wil be calculated and
resulting table will be created as /tmp/butlerStat-PREOPS-XXX.png
file. The text version of the table used to put in Jira comment is
also created as /tmp/butlerStat-PREOPS-XXX.txt

get-panda-stat
--------------

Call::

  prodstat get-panda-stat  inpfile.yaml
  
The input file format is exactly same as for get-butler-stat command.

The program will query PanDa web logs to select information about workflows,
tasks and jobs whose status is either finished, subfinished, running or transforming.
It will produce 2 sorts of tables.

The first one gives the status of the campaign production showing each
workflow status as /tmp/pandaWfStat-PREOPS-XXX.txt.  A styled html
table also is created as /tmp/pandaWfStat-PREOPS-XXX.html

The second table type lists completed tasks, number of quanta in each,
time spent for each job, total time for all quanta and wall time
estimate for each task. This information permit us to estimate rough
number of parallel jobs used for each task, and campaign in whole.
The table names created as /tmp/pandaStat-PREOPS-XXX.png and
pandaStat-PREOPS-XXX.txt.

Here PREOPS-XXX tokens represent Jira ticket the statistics is collected for.

prep-timing-data
-----------------

Call::

  prodstat prep-timing-data ./inp_file.yaml
  
The input yaml file should contain following parameters::

  Jira: "PREOPS-905" - jira ticket corresponding given campaign.
  collType: "2.2i" - a token to help identify campaign workflows.
  bin_width: 3600. - the width of the plot bin in sec.
  job_names - a list of job names
   - 'measure'
   - 'forcedPhotCoad'
   - 'mergeExecutionButler'
  start_at: 0. - plot starts at hours from first quanta
  stop_at: 72. - plot stops at hours from first quanta
  start_date: '2022-02-04' ; dates to select data, which will help to skip previous production steps
  stop_date: '2022-02-07'

The program scan panda idds database to collect timing information for all job names in the list.
Please note the list format for job_names, and the quotes are required around start_date, stop_date.
This can take a long time if there are lots of quanta involved.
Note that the querying of the panDA IDDS can be optimized further in the future.
It creates then timing information in /tmp directory with file names like::

  panda_time_series_<job_name>.csv

plot-data
---------

Call::
  
  prodstat plot-data inp_file.yaml

The program reads timing data created by prep-timing-data command and
build plots for each type of jobs in given time boundaries.
each type of jobs in given time boundaries.
One may change the start_at/stop_at limits to make a zoom in
plot without rerunning prep-timing-data.

report-to-jira
--------------

Call::

   prodstat report-to-jira report.yaml

The report.yaml file provide information about comments and attachments that need to be added or
replaced in given jira ticket.
The structure of the file looks like following::

    project: 'Pre-Operations'
    Jira: PREOPS-905
    comments:
    - file: /tmp/pandaStat-PREOPS-905.txt
    tokens:        tokens to uniquely identify the comment to be replaced
      - 'pandaStat'
      - 'campaign'
      - 'PREOPS-905'
    - file: /tmp/butlerStat-PREOPS-905.txt
    tokens:
      - 'butlerStat'
      - 'PREOPS-905'

 attachments:
  - /tmp/pandaWfStat-PREOPS-905.html
  - /tmp/pandaStat-PREOPS-905.html
  - /tmp/timing_detect_deblend.png
  - /tmp/timing_makeWarp.png
  - /tmp/timing_measure.png
  - /tmp/timing_patch_coaddition.png
