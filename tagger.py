#!/usr/bin/env python

'''
Tags standard product, and co-located products, as validated/invalid/in-progress
'''

import json
import pickle
import hashlib
import requests
from hysds.celery import app
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main():
    '''main function, tags all appropriate ifgs using the given input ifg'''
    #load context & get values
    ctx = load_context()
    coordinates = ctx.get('location')[0].get('coordinates')
    ifg_index = ctx.get('ifg_index')[0]
    orbitNumber = ctx.get('orbitNumber')[0]
    print('orbitnumber: {}'.format(orbitNumber))
    #query AOIs over location
    print('Retrieving AOI\'s over product extent...')
    aois = get_aois(coordinates)
    print('Found AOIs: {}'.format(', '.join([x.get('_id') for x in aois])))
    #for each AOI
    for aoi in aois:
        aoi_name = aoi['_id']
        print('Retrieving products over {}...'.format(aoi_name))
        #query for ACQ-list
        acq_list = get_objects('acq-list', aoi, orbitNumber)
        print('Found {} acquisition-list products.'.format(len(acq_list)))
        #query for IFG
        ifg_list = get_objects('ifg', aoi, orbitNumber, index=ifg_index)
        print('Found {} ifg products.'.format(len(ifg_list)))
        #query for IFG blacklist products
        ifg_blacklist = get_objects('ifg-blacklist', aoi, orbitNumber)
        print('Found {} blacklist products.'.format(len(ifg_blacklist)))
        #if any blacklist products match (list is empty)
        print('Determining matching products...')
        matching_blacklist = return_matching(ifg_blacklist, acq_list)
        if not matching_blacklist:
            #tag all IFG products as <AOI_name>_invalid
            tag = '{0}_invalid'.format(aoi_name)
            tag_all(ifg_list, tag, ifg_index)
        #if all of the ACQ-list are contained in the IFG products
        elif len(return_matching(ifg_list, acq_list)) == len(acq_list):
            #tag all <AOI_name>_validated
            tag = '{0}_validated'.format(aoi_name)
            tag_all(ifg_list, tag, ifg_index)
        #else
            #tag all <AOI_name>_in-progress (if not already)
            tag = '{0}_in-progress'
            tag_all(ifg_list, tag, ifg_index)

def load_context():
    '''loads the context file into a dict'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')

def get_aois(coordinates):
    '''gets AOIs over the given location that have the standard_product machine tag'''
    print('coordinates: {}'.format(coordinates))
    #location['shape']['type'] = 'polygon'
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/grq_*_area_of_interest/_search'.format(grq_ip)
    #grq_query = {"query":{"filtered":{"query":{"bool":{"must":[{"term":{"dataset_type":"area_of_interest"}}]}}, "filter":{"geo_shape":{"location":location}}}}, "fields":["_id", "_source"]}
    grq_query = {"query":{"geo_shape":{"location":{"shape":{"type": "polygon", "coordinates": coordinates}}}}}
    results = query_es(grq_url, grq_query)
    #return only standard product tags
    #std_only = []
    #for prod in results:
    #    if 'standard_product' in prod.get('_source', {}).get('metadata', {}).get('tags', []):
    #        std_only.append(prod)
    #return std_only
    return results

def get_objects(object_type, aoi, orbitNumber, index=None):
    '''returns all objects of the object type ['ifg, acq-list, 'ifg-blacklist'] that intersect both
    temporally and spatially with the aoi'''
    #determine index
    if index is not None:
        idx = index
    else:
        idx_dct = {'ifg':'grq_*_s1-ifg', 'acq-list':'grq_*_acq-list', 'ifg-blacklist':'grq_*_blacklist'}
        idx = idx_dct.get(object_type)
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    #location['type'] = 'polygon'
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    #grq_query = {"query":{"filtered":{"query":{"bool":{"must":[{"range":{"starttime":{"from":starttime, "to":endtime}}}, {"term":{"metadata.orbitNumber":orbitNumber}}]}}, "filter":{"geo_shape":{"location":location}}}}}
    #grq_query = {"query":{"bool":{"must":[{"terms":{"metadata.orbitNumber":orbitNumber}},{"range":{"starttime":{"from":starttime, "to":endtime}}}]}},"from":0,"size":1}
    #grq_query = {"query":{"bool":{"must":[{"terms":{"metadata.orbitNumber":orbitNumber}}, {"range":{"starttime":{"from":starttime, "to":endtime}}}]}},"filter":{"geo_shape":{"location":location}},"from":0,"size":100}
    grq_query = {"query":{"bool":{"must":[{"terms":{"metadata.orbitNumber":orbitNumber}},{"range":{"starttime":{"from":starttime, "to":endtime}}}, {"geo_shape": {"location": location}}]}},"from":0,"size":100}

    results = query_es(grq_url, grq_query)
    return results

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
    #run the query and iterate until all the results have been returned
    print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    print('status code: {}'.format(response.status_code))
    #print('response text: {}'.format(response.text))
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
        response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
        response.raise_for_status()
        results = json.loads(response.text, encoding='ascii')
        results_list.extend(results.get('hits', {}).get('hits', []))
    return results_list

def are_match(es_object1, es_object2):
    '''returns True if the objects share the same set of master/slave scenes, False otherwise'''
    if gen_hash(es_object1) == gen_hash(es_object2):
        return True
    return False

def return_matching(list1, list2):
    '''returns a list of matching objects between the two lists'''
    matching = []
    hashlist1 = build_hashed_dict(list1)
    hashlist2 = build_hashed_dict(list2)
    for key in hashlist1.keys():
        obj = hashlist2.get(key)
        if obj is not None:
            matching.append(obj)
    return matching

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
    master = pickle.dumps(sorted(es_object['_source']['metadata']['master_scenes']))
    slave = pickle.dumps(sorted(es_object['_source']['metadata']['slave_scenes']))
    return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())

def tag_all(object_list, tag, index):
    '''tags all objects in object list with the given tag'''
    for obj in object_list:
        tags = obj.get('_source', {}).get('metadata', {}).get('tags', [])
        tags.append(tag)
        add_tags(index, obj['_id'], tags)
        print('updated {} with tag: {}'.format(obj.get('_id'), tag))

def add_tags(index, uid, tags):
    '''updates the product with the given tag'''
    grq_ip = app.conf['GRQ_ES_URL'].rstrip(':9200').replace('http://', 'https://')
    grq_url = '{0}/{1}/{2}/_update'.format(grq_ip, index, uid)
    es_query = {"doc" : {"metadata": {"tags" : tags}}}
    #print('querying {} with {}'.format(grq_url, es_query))
    response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
    response.raise_for_status()

if __name__ == '__main__':
    main()