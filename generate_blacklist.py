#!/usr/bin/env python

'''
Queries ES for ifg config files. Determines if products
 have been generated for configs. If they have not, and
 jobs have failed n times, generate blacklist products.
'''

from __future__ import print_function
import re
import json
import pickle
import hashlib
import requests
from hysds.celery import app
import build_blacklist_product
#from requests.auth import HTTPBasicAuth
#from hysds_commons.net_utils import get_container_host_ip

def main():
    '''
    Determines all missing ifgs that have ifgs configs and are
    not blacklisted. Checks those products for failed jobs. If
    those jobs are over the count_to_blacklist, it blacklists
    those products.
    '''
    print('Determining variables & ES products...')
    ctx = load_context()
    acq_list_version = ctx['acquisition_list_version']
    count_to_blacklist = ctx['blacklist_at_failure_count']
    acq_lists = build_hashed_dict(get_acq_lists(acq_list_version))
    ifgs = build_hashed_dict(get_ifgs())
    blacklist = build_hashed_dict(get_blacklist())
    print('Found {} acq-lists, {} ifgs, and {} blacklist products.'.format(len(acq_lists.keys()), len(ifgs.keys()), len(blacklist.keys())))
    print('Determining missing IFGs...')
    missing = determine_missing_ifgs(acq_lists, ifgs, blacklist)
    print('Found {} missing IFGs. Checking jobs.'.format(len(missing)))
    add_to_blacklist = determine_failed(missing, count_to_blacklist) #returns a list of acq-list objects that are associated with failed jobs
    print('{} jobs have failed {} times or more. Adding each as a blacklist product...'.format(len(add_to_blacklist), count_to_blacklist))
    for item in add_to_blacklist:
        build_blacklist_product.build(item)

def determine_failed(missing, count_to_blacklist):
    '''
    Determines which acq-list products, which have been filtered by the current
    blacklist, have failed more than count_to_blacklist times. Returns those
    acq-list products. Param missing is the acq-list ES object list.
    '''
    mozart_ip = app.conf['JOBS_ES_URL'].replace('https://', 'http://').rstrip('/')
    mozart_url = '{0}/job_status-current/_search'.format(mozart_ip)

    #es_query = {"query":{"bool":{"must":[{"term":{"status":"job-failed"}},{"term":{"job.job_info.job_payload.job_type":"job-sciflo-s1-ifg"}},{"range":{"job.retry_count":{"gte":count_to_blacklist}}}]}},"from":0,"size":1000}
    if count_to_blacklist > 0:
        es_query = {"query":{"bool":{"must":[{"term":{"status":"job-failed"}},{"term":{"job.job_info.job_payload.job_type":"standard_product-s1gunw-topsapp"}},{"range":{"job.retry_count":{"gte":count_to_blacklist}}}]}},"from":0,"size":1000}
    else:
        es_query = {"query":{"bool":{"must":[{"term":{"job.job_info.job_payload.job_type":"standard_product-s1gunw-topsapp"}},{"term":{"status":"job-failed"}}],"must_not":[],"should":[]}},"from":0,"size":1000,"sort":[],"aggs":{}}
    all_failed = query_es(mozart_url, es_query)
    print('----------------------------------\nall failed jobs: {}\n-------------------------------'.format(all_failed))
    all_failed_dict = build_hashed_dict(all_failed)
    add_to_blacklist = []
    for acq_list in missing:
        if is_in(acq_list, all_failed_dict):
            add_to_blacklist.append(acq_list)
    return add_to_blacklist

def is_in(ifg_cfg, all_failed_dict):
    '''
    Returns True if the ifg_cfg object is inside the failed_job_list. False otherwise.
    '''
    ifg_cfg_hash = gen_hash(ifg_cfg)
    if all_failed_dict.get(ifg_cfg_hash):
        return True
    return False

def determine_missing_ifgs(acq_lists, ifgs, blacklist):
    '''
    Determines the ifgs that have not been produced from the acquisition lists.
    '''
    missing = []
    for key in acq_lists.keys():   
        print('checking for: {}'.format(key))
        if not key in ifgs and not key in blacklist:
            missing.append(acq_lists[key])
    return missing

def build_hashed_dict(object_list):
    '''
    Builds a dict of the object list where the keys are a hashed object of each objects
    master and slave list. Returns the dict.
    '''
    hashed_dict = {}
    for obj in object_list:
        hashed_dict.update({gen_hash(obj):obj})
    return hashed_dict

def gen_hash(es_object):
    '''Generates a hash from the master and slave scene list'''
    met = es_object.get('_source', {}).get('metadata', {})
    master = [get_starttime(x) for x in met.get('master_scenes', [])]
    slave = [get_starttime(x) for x in met.get('slave_scenes', [])]
    master = pickle.dumps(sorted(master))
    slave = pickle.dumps(sorted(slave))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())

def get_starttime(input_string):
    '''returns the starttime from the input string. Used for comparison of acquisition ids to SLC ids'''
    st_regex = '([1-2][0-9]{7}T[0-2][0-9][0-6][0-9][0-6][0-9])'
    result = re.search(st_regex, input_string)
    try:
        starttime = result.group(0)
        return starttime
    except Exception, err:
        raise Exception('input product: {} does not match regex:{}. Cannot compare SLCs to acquisition ids. {}'.format(input_string, st_regex, err))

def get_ifgs():
    '''
    Returns all ifg products on ES
    '''
    grq_ip = app.conf['GRQ_ES_URL'].rstrip(':9200').replace('http://', 'https://')
    grq_url = '{0}/es/grq_*_s1-gunw/_search'.format(grq_ip)
    es_query = {"query":{"bool":{"must":[{"match_all":{}}]}}, "from":0, "size":1000}
    return query_es(grq_url, es_query)

def get_acq_lists(acq_version):
    '''Returns all acquisition-list products on ES matching the ifg_version'''
    grq_ip = app.conf['GRQ_ES_URL'].rstrip(':9200').replace('http://', 'https://')
    grq_url = '{0}/es/grq_{1}_s1-gunw-acq-list/_search'.format(grq_ip, acq_version)
    es_query = {"query":{"bool":{"must":[{"match_all":{}}]}}, "from":0, "size":1000}
    return query_es(grq_url, es_query)

def get_blacklist():
    '''Returns all blacklist products'''
    grq_ip = app.conf['GRQ_ES_URL'].rstrip(':9200').replace('http://', 'https://')
    grq_url = '{0}/es/grq_*_s1-gunw-ifg-blacklist/_search'.format(grq_ip)
    es_query = {"query":{"bool":{"must":[{"match_all":{}}]}}, "from":0, "size":1000}
    return query_es(grq_url, es_query)

def query_es(grq_url, es_query):
    '''
    Runs the query through Elasticsearch, iterates until
    all results are generated, & returns the compiled result
    '''
    # make sure the fields from & size are in the es_query
    if 'size' in es_query.keys():
        iterator_size = es_query['size']
    else:
        iterator_size = 1000
        es_query['size'] = iterator_size
    if 'from' in es_query.keys():
        from_position = es_query['from']
    else:
        from_position = 0
        es_query['from'] = from_position
    # run the query and iterate until all the results have been returned
    print('querying: {}'.format(grq_url))
    response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results['hits']['hits']
    total_count = results['hits']['total']
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        results_list.extend(results['hits']['hits'])
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
