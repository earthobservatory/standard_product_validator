#!/usr/bin/env python

'''
Builds a s1-ifg-blacklist product from an ifg-cfg product
'''

import os
import json
import shutil
import pickle
import hashlib
from hysds.celery import app
from hysds.dataset_ingest import ingest

VERSION = 'v1.0'
PRODUCT_PREFIX = 'S1-ifg-cfg-blacklist'


def build(ifg_cfg):
    '''Builds and submits a s1-ifg-blacklist product from an ifg_cfg.'''
    ds = build_dataset(ifg_cfg)
    met = build_met(ifg_cfg)
    build_product_dir(ds, met)
    submit_product(ds)
    print('Publishing Product: {0}'.format(ds['label']))
    print('    version:        {0}'.format(ds['version']))
    print('    starttime:      {0}'.format(ds['starttime']))
    print('    endtime:        {0}'.format(ds['endtime']))
    print('    location:       {0}'.format(ds['location']))
    print('    master_scenes:  {0}'.format(met['master_scenes']))
    print('    slave_scenes:   {0}'.format(met['slave_scenes']))

def build_id(s1_ifg):
    global VERSION
    global PRODUCT_PREFIX
    hsh = gen_hash(s1_ifg)
    uid = '{}_{}_{}'.format(PRODUCT_PREFIX, hsh, VERSION)
    return uid

def gen_hash(es_object):
    '''Generates a hash from the master and slave scene list'''
    master = pickle.dumps(sorted(es_object['_source']['metadata']['master_scenes'], -1))
    slave = pickle.dumps(sorted(es_object['_source']['metadata']['slave_scenes'], -1))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())

def build_dataset(ifg_cfg):
    '''Generates the ds dict for the ifg-cfg-blacklist from an ifg-cfg'''
    uid = build_id(ifg_cfg)
    starttime = ifg_cfg['_source']['metadata']['starttime']
    endtime = ifg_cfg['_source']['metadata']['endtime']
    location = ifg_cfg['_source']['metadata']['union_geojson']
    global VERSION
    ds = {'label':uid, 'starttime':starttime, 'endtime':endtime, 'location':location, 'version':VERSION}
    return ds

def build_met(ifg_cfg):
    '''Generates the met dict for the ifg-cfg-blacklist from an ifg-cfg'''
    master_list = ifg_cfg['_source']['metadata']['master_ids']
    slave_list = ifg_cfg['_source']['metadata']['slave_ids']
    track = ifg_cfg['_source']['metadata']['track']
    met = {'master_scenes': master_list, 'slave_scenes': slave_list, 'track': track}
    return met

def build_product_dir(ds, met):
    label = ds['label']
    ds_dir = os.path.join(os.getcwd(), label)
    ds_path = os.path.join(ds_dir, '{0}.dataset.json'.format(label))
    met_path = os.path.join(ds_dir, '{0}.met.json'.format(label))
    if not os.path.exists(ds_dir):
        os.mkdir(ds_dir)
    with open(ds_path, 'w') as outfile:
        json.dump(ds, outfile)
    with open(met_path, 'w') as outfile:
        json.dump(met, outfile)

def submit_product(ds):
    uid = ds['label']
    ds_dir = os.path.join(os.getcwd(), uid)
    try:
        ingest(uid, './datasets.json', app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, ds_dir, None)
        if os.path.exists(uid):
            shutil.rmtree(uid)
    except Exception, err:
        print('failed on submission of {0} with {1}'.format(uid, err))
