#!/usr/bin/env python

import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch(hosts=['10.128.0.2'])

if (len(sys.argv)<2):
	print ('usage ' + sys.argv[0] + ' <file>')
	exit()

batch_size = 100000

result_dict = {
		'P'  :'Pass',
		'F'  :'Fail',
		'PRS':'Pass_rectified_at_station',
		'ABA':'Abandon',
		'ABR':'Abort',
		'R'  :'Refusal_to_test'}

fuel_dict = {
		'P':'Petrol',
		'D':'Diesel',
		'E':'Electric',
		'S':'Steam',
		'L':'Liquified_petroleum_gas',
		'C':'Compressed_natural_gas',
		'N':'Liquified_natural_gas',
		'F':'Fuel_cells',
		'O':'Other'}


with open(sys.argv[1]) as f:
	doc_id=0;
	bulk_doc=[];
	for line in f:
		s = ('' + line).split('|')
		doc={}
		doc['test_date']   = s[2]
		doc['test_class']  = s[3]
		doc['test_type']   = s[4]
		doc['result']      = result_dict[s[5]]
		doc['odometer']    = int(s[6])
		#doc['postcode']    = s[7]
		doc['make']        = s[8].replace(' ','_')
		doc['model']       = s[9].replace(' ','_')
		doc['colour']      = s[10].replace(' ','_')
		doc['fuel']        = fuel_dict[s[11]]
		doc['engine_size'] = int(s[12])
		doc['date_of_registration'] = s[13].replace('\r','').replace('\n','')
		if (doc['date_of_registration'] == 'NULL'):
			doc['date_of_registration'] = '1970-01-01'
		doc['_index']   = 'mot'
		doc['_type']    = 'test'
		doc_id += 1
		bulk_doc.append(doc)
		if ((doc_id % batch_size) == 0):
			try:
				print ('indexing docs, up to ' + str(doc_id))
				helpers.bulk(es, bulk_doc)
				bulk_doc = []
			except:
				print ('Failed to insert ' + bulk_doc)
				bulk_doc = []
				continue
	# Write final odd batch
	if (len(bulk_doc)>0):
		try:
			print ('indexing docs, up to ' + str(doc_id))
			helpers.bulk(es, bulk_doc)
			bulk_doc = []
		except:
			print ('Failed to insert ' + bulk_doc)
			bulk_doc = []

