#!/usr/bin/env python

'''
From an input hysds job, attempts to generate a blacklist product for the expected product
from that job.
'''

from __future__ import print_function
import json
import hashlib
import requests

import build_blacklist_product
from hysds.celery import app

def main():
    '''
    Pulls the job info from context, and generates appropriate blacklist products for
    the given job.
    '''
    print('Loading variables from context...')
    ctx = load_context()
    required_retry_count = int(ctx.get('required_retry_count', 0))
    current_retry_count = ctx.get('current_retry_count', 0)
    if isinstance(current_retry_count, list):
        current_retry_count = current_retry_count[0] # if it's a list get the first item (will return list as lambda)
    master_slcs = ctx.get('master_slcs', False)
    slave_slcs = ctx.get('slave_slcs', False)
    #check if job retry counts are appropriate
    if current_retry_count < required_retry_count:
        print('current job retry_count of {} less than the required of {}. Exiting.'.format(current_retry_count, required_retry_count))
        return
    # check if master/slave scenes are appropriate
    if master_slcs is False or slave_slcs is False:
        print('master/slave metadata fields are not included in job met. Exiting.')
        return
    # get the associated ifg-cfg list corresponding to the failed job
    print('querying for appropriate ifg-cfg...')
    ifg_cfg = get_ifg_cfg(master_slcs, slave_slcs)
    print('ifg found: {}'.format(ifg_cfg))
    print('building blacklist product')
    build_blacklist_product.build(ifg_cfg)

def get_ifg_cfg(master_slcs, slave_slcs):
    '''es query for the associated ifg-cfg'''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/grq_*_s1-gunw-ifg-cfg/_search'.format(grq_ip)
    hsh = gen_direct_hash(master_slcs, slave_slcs)
    es_query = {"query":{"bool":{"must":[{"term":{"metadata.full_id_hash.raw":hsh}}]}},"from":0,"size":10}
    print('es query: {}'.format(json.dumps(es_query)))
    results = query_es(grq_url, es_query)
    return results[0]

def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    # make sure the fields from & size are in the es_query
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 10
        es_query['size'] = iterator_size
    if 'from' in es_query.keys():
        from_position = es_query['from']
    else:
        from_position = 0
        es_query['from'] = from_position
    #run the query and iterate until all the results have been returned
    print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))
    return results_list

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
    return gen_direct_hash(master_slcs, slave_slcs)

def gen_direct_hash(master_slcs, slave_slcs):
    '''generates hash directly from slcs'''
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


def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')

if __name__ == '__main__':
    main()
