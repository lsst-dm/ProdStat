from yaml import load,dump

import sys

tocheck=sys.argv[1]

drpfile=open('DRP.yaml')
drpyaml=load(drpfile)

#eventually need to load the includes for more details

taskdict={}
stepdict={}
stepdesdict={}
subsets=drpyaml['subsets']
for k,v in subsets.items():
   stepname=k
   tasklist=v['subset']
   for t in tasklist:
     taskdict[t]=stepname
   stepdict[stepname]=tasklist
   stepdesdict[stepname]=v['description']

#assumes tasknames are unique 
#i.e. that there's not more than one step
#with the same taskname
#print("steps")
#for k,v in stepdict.items():
 #print(k,v)
 #print(stepdesdict[k])

if tocheck in stepdict:
 print(stepdict[tocheck])
 print(stepdesdict[tocheck])
elif tocheck in taskdict:
 print(tocheck,taskdict[tocheck])
 


