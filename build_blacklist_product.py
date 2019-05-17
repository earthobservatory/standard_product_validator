#!/usr/bin/env python

'''
Builds a s1-ifg-blacklist product from an ifg-cfg product
'''

from __future__ import print_function
import os
import json
import shutil
import hashlib
import dateutil.parser
from hysds.celery import app
from hysds.dataset_ingest import ingest

VERSION = 'v1.0'
PRODUCT_PREFIX = 'S1-GUNW-BLACKLIST'


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

def build_id(ifg):
    global VERSION
    global PRODUCT_PREFIX
    hsh = gen_hash(ifg)
    master_date = get_master_date(ifg)
    slave_date = get_slave_date(ifg)
    uid = '{}-{}_{}-{}-{}'.format(PRODUCT_PREFIX, master_date, slave_date, hsh, VERSION)
    return uid

def get_hash(es_obj):
    '''retrieves the full_id_hash. if it doesn't exists, it
        attempts to generate one'''
    full_id_hash = es_obj.get('_source', {}).get('metadata', {}).get('full_id_hash', False)
    if full_id_hash:
        return full_id_hash
    return gen_hash(es_obj)

def gen_hash(es_obj):
    '''copy of hash used in the enumerator'''
    met = es_obj.get('_source', {}).get('metadata', {})
    master_slcs = met.get('master_scenes', met.get('reference_scenes', False))
    slave_slcs = met.get('slave_scenes', met.get('secondary_scenes', False))
    master_ids_str = ""
    slave_ids_str = ""
    for slc in sorted(master_slcs):
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]
        if master_ids_str == "":
            master_ids_str = slc
        else:
            master_ids_str += " "+slc
    for slc in sorted(slave_slcs):
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]
        if slave_ids_str == "":
            slave_ids_str = slc
        else:
            slave_ids_str += " "+slc
    id_hash = hashlib.md5(json.dumps([master_ids_str, slave_ids_str]).encode("utf8")).hexdigest()
    return id_hash

def get_master_date(ifg_cfg):
    '''returns the master date'''
    return dateutil.parser.parse(ifg_cfg.get('_source').get('endtime')).strftime('%Y%m%d')

def get_slave_date(ifg_cfg):
    '''returns the master date'''
    return dateutil.parser.parse(ifg_cfg.get('_source').get('starttime')).strftime('%Y%m%d')

def build_dataset(ifg_cfg):
    '''Generates the ds dict for the ifg-cfg-blacklist from an ifg-cfg'''
    uid = build_id(ifg_cfg)
    starttime = ifg_cfg['_source']['metadata']['starttime']
    endtime = ifg_cfg['_source']['metadata']['endtime']
    location = ifg_cfg['_source']['metadata']['union_geojson']
    ds = {'label':uid, 'starttime':starttime, 'endtime':endtime, 'location':location, 'version':VERSION}
    return ds

def build_met(ifg_cfg):
    '''Generates the met dict for the ifg-cfg-blacklist from an ifg-cfg'''
    met = ifg_cfg.get('_source', {}).get('metadata', {})
    master_scenes = met.get('master_scenes', met.get('reference_scenes', False))
    slave_scenes = met.get('slave_scenes', met.get('secondary_scenes', False))
    track = ifg_cfg['_source']['metadata'].get('track_number', False)
    if track is False:
        track = ifg_cfg['_source']['metadata'].get('track', False)
    master_orbit_file = ifg_cfg['_source']['metadata'].get('master_orbit_file', False)
    slave_orbit_file = ifg_cfg['_source']['metadata'].get('slave_orbit_file', False)
    hsh = get_hash(ifg_cfg)
    met = {'reference_scenes': master_scenes, 'secondary_scenes': slave_scenes,
           'master_orbit_file': master_orbit_file, 'slave_orbit_file': slave_orbit_file, 'track_number': track,
    'full_id_hash': hsh}
    return met

def build_product_dir(ds, met):
    '''generates the blacklist product'''
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
    except Exception:
        print('failed on submission of {0}'.format(uid))
