#!/usr/bin/env python

'''
From an input hysds job, attempts to generate a blacklist product for the expected product
from that job.
'''

from __future__ import print_function
import json
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
    master_slcs = ctx.get('master_slcs', False)[0]
    slave_slcs = ctx.get('slave_slcs', False)[0]
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
    print('slave slcs: {}'.format(slave_slcs))
    #build es query
    master_term = ','.join([json.dumps({"term":{"metadata.master_slcs.raw": x}}) for x in master_slcs])
    slave_term = ','.join([json.dumps({"term":{"metadata.slave_slcs.raw": x}}) for x in slave_slcs])
    es_query = json.loads('{"query":{"bool":{"must":[' + master_term + ',' + slave_term + ']}},"from":0,"size":10}')
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
