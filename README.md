#ProdStat
###### This repository contains some example scripts to collect info from Butler and from PanDa
### GetButlerStat.py
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
Here workflows represent a list of time stamps that are part of workflow name,
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
