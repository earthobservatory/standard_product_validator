#!/usr/bin/env python

'''
Tags standard product, and co-located products, as validated/invalid/in-progress
'''

import re
import json
import pickle
import hashlib
import requests
from hysds.celery import app
import urllib3
from collections import OrderedDict
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main():
    '''main function, tags all appropriate ifgs using the given input ifg'''
    #load context & get values
    ctx = load_context()
    coordinates = ctx.get('location').get('coordinates')
    ifg_index = ctx.get('ifg_index')
    orbitNumber = ctx.get('orbitNumber')
    aoi_name = ctx.get('AOI', False)
    print('orbitnumber: {}'.format(orbitNumber))
    #query AOIs over location
    print('Retrieving AOI\'s over product extent...')
    aois = get_aois(coordinates)
    if aoi_name:
        print('Enumerating over AOI {} only.'.format(aoi_name))
        aois = [x for x in aois if x.get('_id', '') == aoi_name] #filter out other AOIs
    print('Found AOIs: {}'.format(', '.join([x.get('_id') for x in aois])))
    #for each AOI
    for aoi in aois:
        aoi_name = aoi['_id']
        print('\nRetrieving products over {}...\n-----------------------'.format(aoi_name))
        #query for ACQ-list
        acq_list = get_objects('acq-list', aoi, orbitNumber)
        print('Found {} acquisition-list products.'.format(len(acq_list)))
        if len(acq_list) == 0:
            print('Since 0 acq-list products have been found, ending AOI tagging.')
            continue
        #query for IFG
        ifg_list = get_objects('ifg', aoi, orbitNumber, index=ifg_index)
        print('Found {} ifg products.'.format(len(ifg_list)))
        #query for IFG blacklist products
        ifg_blacklist = get_objects('ifg-blacklist', aoi, orbitNumber)
        print('Found {} blacklist products.'.format(len(ifg_blacklist)))
        #if any blacklist products match (list is empty)
        print('Determining matching products...')
        matching_blacklist = return_matching(ifg_blacklist, acq_list)
        if len(matching_blacklist) > 0:
            #tag all IFG products as <AOI_name>_invalid
            print('Found matching blacklist products. Tagging as invalid.')
            tag = '{0}_invalid'.format(aoi_name)
            tag_all(ifg_list, tag, ifg_index, aoi_name)
        elif contains(ifg_list, acq_list):
            #if all of the ACQ-list are contained in the IFG products
            #tag all <AOI_name>_validated
            print('All input acq-lists are contained by the ifg products. Tagging as validated')
            tag = '{0}_validated'.format(aoi_name)
            tag_all(ifg_list, tag, ifg_index, aoi_name)
        else:
            #tag all <AOI_name>_in-progress (if not already)
            print('Missing ifg products from acq-lists. Tagging as in-progress')
            print('Missing acq-list Products:\n------------------')
            missing = return_missing(ifg_list, acq_list)
            ids = [x['_id'] for x in missing]
            for i in ids:
                print(i)
            tag = '{0}_in-progress'.format(aoi_name)
            tag_all(ifg_list, tag, ifg_index, aoi_name)

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
        idx_dct = {'ifg':'grq_*s1-gunw', 'acq-list':'grq_*_acq-list', 'ifg-blacklist':'grq_*_blacklist'}
        idx = idx_dct.get(object_type)
    starttime = aoi.get('_source', {}).get('starttime')
    endtime = aoi.get('_source', {}).get('endtime')
    location = aoi.get('_source', {}).get('location')
    #location['type'] = 'polygon'
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/_search'.format(grq_ip, idx)
    grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},"filter":{"bool":{"must":[{"term":{"metadata.orbitNumber":orbitNumber[0]}},{"term":{"metadata.orbitNumber":orbitNumber[1]}},{"range":{"starttime":{"from":starttime,"to":endtime}}}]}}}},"from":0,"size":100}
    if object_type == 'ifg':
        #orbitNumber has been updated to orbit_number in ifg metadata
        grq_query = {"query":{"filtered":{"query":{"geo_shape":{"location": {"shape":location}}},"filter":{"bool":{"must":[{"term":{"metadata.orbit_number":orbitNumber[0]}},{"term":{"metadata.orbit_number":orbitNumber[1]}},{"range":{"starttime":{"from":starttime,"to":endtime}}}]}}}},"from":0,"size":100}
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
    #print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
    response = requests.post(grq_url, data=json.dumps(es_query), verify=False)
    #print('status code: {}'.format(response.status_code))
    #print('response text: {}'.format(response.text))
    response.raise_for_status()
    results = json.loads(response.text, encoding='ascii')
    results_list = results.get('hits', {}).get('hits', [])
    total_count = results.get('hits', {}).get('total', 0)
    for i in range(iterator_size, total_count, iterator_size):
        es_query['from'] = i
        #print('querying: {}\n{}'.format(grq_url, json.dumps(es_query)))
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

def contains(list1, list2):
    '''returns True if list1 contains all products in list2. False otherwise'''
    hashlist1 = build_hashed_dict(list1)
    hashlist2 = build_hashed_dict(list2)
    for key in hashlist2.keys():
        if hashlist1.get(key, False) is False:
            return False
    return True

def return_missing(list1, list2):
    '''returns the products in list2 that are NOT contained by list1'''
    missing = []
    hashlist1 = build_hashed_dict(list1)
    hashlist2 = build_hashed_dict(list2)
    for key in hashlist2.keys():
        obj = hashlist1.get(key)
        if obj is None:
            missing.append(hashlist2[key])
    return missing

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
    #master = pickle.dumps(sorted(es_object['_source']['metadata']['master_scenes']))
    #slave = pickle.dumps(sorted(es_object['_source']['metadata']['slave_scenes']))
    #return '{}_{}'.format(hashlib.md5(master).hexdigest(), hashlib.md5(slave).hexdigest())
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
        raise Exception('input product: {} does not match regex:{}. Cannot compare SLCs to acquisition ids.'.format(input_string, st_regex))

def tag_all(object_list, tag, index, aoi_name):
    '''tags all objects in object list with the given tag'''
    for obj in object_list:
        tags = get_current_tags(obj) #gets the current tags from es. this is needed bc we do mulitple updates
        #remove any that might be already tagged
        remove_tags = ['{0}_in-progress'.format(aoi_name), '{0}_validated'.format(aoi_name), '{0}_invalid'.format(aoi_name)]
        tags = [x for x in tags if x not in remove_tags]
        tags.append(tag)
        prod_type = obj['_type']
        add_tags(index, obj['_id'], prod_type, list(set(tags)))
        print('updated {} with tag: {}'.format(obj.get('_id'), tag))

def add_tags(index, uid, prod_type, tags):
    '''updates the product with the given tag'''
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/{2}/{3}/_update'.format(grq_ip, index, prod_type, uid)    
    es_query = {"doc" : {"metadata": {"tags" : tags}}}
    #print('querying {} with {}'.format(grq_url, es_query))
    response = requests.post(grq_url, data=json.dumps(es_query), timeout=60, verify=False)
    response.raise_for_status()

def get_current_tags(obj):
    '''gets the current tags of the object'''
    uid = obj.get('_id')
    prod_type = obj.get('_type')
    index = obj.get('_index')
    grq_ip = app.conf['GRQ_ES_URL'].replace(':9200', '').replace('http://', 'https://')
    grq_url = '{0}/es/{1}/{2}/_search'.format(grq_ip, index, prod_type)
    grq_query = {"query": {"bool": {"must": {"match": {"_id": uid}}}}}
    results = query_es(grq_url, grq_query)
    tags = results[0].get('_source', {}).get('metadata', {}).get('tags', [])
    print('current tags: {}'.format(tags))
    return tags


if __name__ == '__main__':
    main()
