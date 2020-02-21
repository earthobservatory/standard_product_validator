#!/usr/bin/env python

'''
Submits an enumeration job from an input blacklist product,
for all AOIs and POEORBS covered by the product.
'''

from __future__ import print_function
import json
import requests
from hysds.celery import app
import submit_job

ALLOWED_PROD_TYPES = ['S1-GUNW-BLACKLIST']
AUDIT_TRAIL_IDX = 'grq_*_s1-gunw-acqlist-audit_trail'
POEORB_IDX = 'grq_*_s1-aux_poeorb'

def main():
    '''main loop'''
    #stage inputs
    ctx = load_context()
    #dataset inputs
    prod_type = ctx.get('prod_type', False)
    poeorb_id = ctx.get('master_orbit_file', False)
    full_id_hash = ctx.get('full_id_hash', False)
    # user inputs
    skip_days = ctx.get('skipDays', 0)
    minmatch = ctx.get('minMatch', 2)
    queue = ctx.get('enumerator_queue', 'standard_product-s1gunw-acq_enumerator')
    version = ctx.get('enumeration_job_version', 'master')
    acquisition_version = ctx.get('acquisition_version')
    if not prod_type in ALLOWED_PROD_TYPES:
        raise Exception('Product type of {} not allowed as input.'.format(prod_type))
    #determine covered aois
    aois = get_aois(full_id_hash)
    print('found {} aoi(s) covering blacklist: {}'.format(len(aois), ', '.join(aois)))
    #get track
    track = get_track(full_id_hash)
    #get poeorb object
    poeorb = get_poeorb(poeorb_id)
    #submit enumeration job per aoi
    for aoi in aois:
        print('submitting enumeration job for poeorb id: {}, over aoi: {}, with track: {}'.format(poeorb_id, aoi, track))
        submit_enum_job(poeorb, aoi, track, queue, version, minmatch, acquisition_version, skip_days, False)

def get_aois(full_id_hash):
    '''determines all aois covered by the given hash'''
    aois = []
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, AUDIT_TRAIL_IDX)
    must = [{"term": {"metadata.full_id_hash.raw": full_id_hash}}]
    grq_query = {"query": {"filtered": {'filter': {"bool": {"must": must}}}}, "from": 0, "size": 20}
    audit_trails = query_es(grq_url, grq_query)
    for audit in audit_trails:
        aoi = audit.get('_source', {}).get('metadata', {}).get('aoi', False)
        if aoi and aoi not in aois:
            aois.append(aoi)
    return aois

def get_track(full_id_hash):
    '''determines the track covered by the given hash'''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, AUDIT_TRAIL_IDX)
    must = [{"term": {"metadata.full_id_hash.raw": full_id_hash}}]
    grq_query = {"query": {"filtered": {'filter': {"bool": {"must": must}}}}, "from": 0, "size": 20}
    audit_trails = query_es(grq_url, grq_query)
    for audit in audit_trails:
        track = audit.get('_source', {}).get('metadata', {}).get('track_number', False)
        if track:
            return track
    raise Exception('no audit trail product found. Unable to determine appropriate track.')

def get_poeorb(poeorb_id):
    '''returns the poeorb es object'''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, POEORB_IDX)
    must = [{"term": {"metadata.archive_filename.raw": poeorb_id}}]
    grq_query = {"query": {"filtered": {'filter': {"bool": {"must": must}}}}, "from": 0, "size": 20}
    poeorbs = query_es(grq_url, grq_query)
    if not poeorbs:
        raise Exception('no audit poeorbn product found. Unable to submit enumeration job.')
    return poeorbs[0]

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
    #print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
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

def submit_enum_job(poeorb, aoi, track, queue, job_version, minmatch, acquisition_version, skip_days, enable_dedup):
    '''submits an enumeration job for the give poeorb, aoi, & track. if track is false, it does not use that parameter'''
    job_name = "job-standard_product-s1gunw-acq_enumerator"
    priority = 5
    tags = 'enumeration_from_blacklist'
    job_params = {
        "aoi_name": aoi,
        "workflow": "orbit_acquisition_enumerator_standard_product.sf.xml",
        "project": "grfn",
        "dataset_version": "v2.0.0",
        "minMatch": minmatch,
        "threshold_pixel": 5,
        "acquisition_version": acquisition_version,
        "track_numbers": str(track),
        "skipDays": skip_days,
        "starttime": poeorb.get('_source', {}).get('starttime', False),
        "endtime": poeorb.get('_source', {}).get('endtime', False),
        "platform": poeorb.get('_source').get('metadata').get('platform'),
        "localize_products": poeorb.get('_source').get('urls')[1]
    }
    submit_job.main(job_name, job_params, job_version, queue, priority, tags, enable_dedup=enable_dedup)

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
