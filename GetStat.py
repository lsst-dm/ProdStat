#!/usr/bin/env python
# coding: utf-8
import sys
import re
from collections import defaultdict
import multiprocessing
import yaml
import numpy as np
import pandas as pd
#import psycopg2
from lsst.daf.butler import Butler
from lsst.daf.butler import ButlerURI
" Need this to have LSST yaml schemas "
import lsst.daf.base as base
import lsst.daf.base.yaml 


def parse_metadata_yaml(yaml_file):
    """
    Parse the runtime and RSS data in the metadata yaml file created
    by the lsst.pipe.base.timeMethod decorator as applied to pipetask
    methods.
    """
    time_types = 'Cpu User System'.split()
    min_fields = [f'Start{_}Time' for _ in time_types]
    max_fields = [f'End{_}Time' for _ in time_types] + ['MaxResidentSetSize']

    results = dict()
    with open(yaml_file) as fd:
        md = yaml.safe_load(fd)
    methods = list(md.keys())
#    print(md)
#    print("Methods ",methods)
    for method in methods:
        for key, value in md[method].items():
            for min_field in min_fields:
                if not key.endswith(min_field):
                    continue
                if min_field not in results or value < results[min_field]:
                    results[min_field] = value
                    continue
            for max_field in max_fields:
                if not key.endswith(max_field):
                    continue
                if max_field not in results or value > results[max_field]:
                    results[max_field] = value
                    continue
    return results


REPO_ROOT = 's3://butler-us-central1-panda-dev/hsc/butler-external.yaml'
butler = Butler(REPO_ROOT)
registry = butler.registry

" Select collections and sort them per user creating dictionary with username and list of collections"
collections=[]
users=[]
for c in sorted(registry.queryCollections()):
    if str(c).startswith('u/'):
        tokens = str(c).split('/')
        username = tokens[1]
        if username not in users:
            users.append(username)
        if len(tokens) >3:
            collections.append(c)
userColl = {}
for user in users:
    ucol=[]
    for c in collections:
        if str(c).split('/')[1] == user:
            ucol.append(c)
    userColl[user] = ucol
print(userColl)
print(' ')

" Recreate Butler and registry "
butler = Butler(REPO_ROOT, collections=collections)
registry = butler.registry

" We can collect datasets and data IDs for each user's collection "
datatype_pattern='.*_metadata'
pattern = re.compile(datatype_pattern)

userCollData = dict()
for user in userColl:
    _collections = userColl[user]
    _collData = dict()
    for collection in _collections:
        datasetRefs = []
        _datasetRefs = registry.queryDatasets(pattern, collections=collection)
        dataIds =[]
        for i, ref in enumerate(_datasetRefs):
            _id= ref.dataId
            if len(_id) > 0:
                if _id not in dataIds: dataIds.append(_id)
            if i >2:
                break
        _collData[collection] = (dataIds,_datasetRefs)
    userCollData[user] = _collData


"""
Process a list of datarefs, extracting the per-task resource usage
info from the `*_metadata` yaml files.
"""
verbose=True
columns = ('detector', 'tract', 'patch', 'band', 'visit')
data = defaultdict(list)
" Just for one user "
#users=['hneal2']
users=['huanlin']
for user in users:
#    print("User Data:", userCollData[user])
    _collections = userColl[user]
    for collection in _collections:
        data = defaultdict(list)
        (dataIds,dataRefs) = (userCollData[user])[collection]
        print('collection=',collection)
        
        for i, dataref in enumerate(dataRefs):
            if verbose:
                if i % 20 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
            try:
                refyaml = butler.getURI(dataref, collections=collection)
            except:
                print('Yaml file not found skiping')
                continue
            dest = ButlerURI('~/MyFile.yaml')
            buri = ButlerURI(refyaml)
            if not buri.exists():
                print('The file do not exists')
            local = dest
            remote=buri
            data['task'].append(dataref.datasetType.name[:-len('_metadata')])
#        print('task: ',data['task'])
            dataId = dict(dataref.dataId)
#        print("dataId ",dataId)
            if 'visit' not in dataId and 'exposure' in dataId:
                dataId['visit'] = dataId['exposure']
            for column in columns:
                data[column].append(dataId.get(column, None))
#
            dest.transfer_from(buri,'copy',True)
            results = parse_metadata_yaml('MyFile.yaml')
            data['cpu_time'].append(results.get('EndCpuTime', None))
            data['maxRSS'].append(results.get('MaxResidentSetSize', None))
#
        print(' ')
        print(user,'\n')
#
        print(" collection: ",collection)
        print("data")

#    print(data.keys())
        for key in data.keys():
            print(key,':',data[key])
            print(' ')
# 
"""
    dest = ButlerURI('/home/kuropat/WORK/')
    dataset_type='calexp'
    find_first=True
    preserve_path=False
    clobber=True
    refs = list(butler.registry.queryDatasets(datasetType=dataset_type,
                                              collections=collections,
                                              where='',
                                              findFirst=find_first))
    print('refs=',refs)
#    log.info("Number of datasets matching query: %d", len(refs))

    transferred = butler.retrieveArtifacts(refs, destination=dest, transfer='copy',
                                           preserve_path=preserve_path, overwrite=clobber)
"""
