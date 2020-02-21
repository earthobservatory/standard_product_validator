#!/usr/bin/env python

'''
From an input hysds job, attempts to generate a greylist product for the expected product
from that job.
'''

from __future__ import print_function
import json
import os, sys
import hashlib
import requests

import build_greylist_product
from hysds.celery import app

GRQ_URL = app.conf.GRQ_ES_URL

def get_dataset_by_hash(ifg_hash, es_index="grq"):
    """Query for existence of dataset by ID."""

    es_url = GRQ_URL

    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{"metadata.full_id_hash.raw": ifg_hash} },
                    { "term":{"dataset.raw": "S1-GUNW-GREYLIST"} }
                ]
            }
        }
        
    }

    print(query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    print("search_url : %s" %search_url)

    r = requests.post(search_url, data=json.dumps(query))
    r.raise_for_status()

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        raise RuntimeError("Failed to query %s:\n%s" % (es_url, r.text))
    result = r.json()
    print(result['hits']['total'])
    return result

def check_ifg_status_by_hash(new_ifg_hash):
    es_index="grq_*_s1-gunw-greylist"
    result = get_dataset_by_hash(new_ifg_hash, es_index)
    total = result['hits']['total']
    print("check_slc_status_by_hash : total : %s" %total)
    if total>0:
        found_id = result['hits']['hits'][0]["_id"]
        print("Duplicate Greylist dataset found: %s" %found_id)
        sys.exit(0)

    print("check_slc_status : returning False")
    return False

def main():
    '''
    Pulls the job info from context, and generates appropriate greylist products for
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
    hsh = gen_direct_hash(master_slcs, slave_slcs)
    if check_ifg_status_by_hash(hsh):
        err = "S1-GUNW-GREYLIST Found with full_hash_id : %s" %hsh
        print(err)
        sys.exit(0)

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
    print('building greylist product')
    build_greylist_product.build(ifg_cfg)

def get_ifg_cfg(master_slcs, slave_slcs):
    '''es query for the associated ifg-cfg'''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/grq_*_s1-gunw-ifg-cfg/_search'.format(grq_ip)
    hsh = gen_direct_hash(master_slcs, slave_slcs)
    es_query = {"query":{"bool":{"must":[{"term":{"metadata.full_id_hash.raw":hsh}}]}},"from":0,"size":10}
    print('es query: {}'.format(json.dumps(es_query)))
    results = query_es(grq_url, es_query)
    if len(results)<1:
        raise RuntimeError("Failed to get ifg_cfg with full_id_hash : {}".format(hsh))
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
