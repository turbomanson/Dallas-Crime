#!/usr/bin/env python

import os
import pandas as pd
from sodapy import Socrata

def download_save(domain, token, dataset_id, path, file_name):
	client = Socrata(domain, token)
	results = client.get(dataset_id, limit=500000)
	df = pd.DataFrame.from_dict(results)
	df.to_csv(path+file_name)

def main():
	APP_TOKEN = os.environ.get("SODA_DALLAS_APPTOKEN")
	DOMAIN = "www.dallasopendata.com"
	DATASET_ID = "tbnj-w5hb"
	DATASET_ID2 = "qqc2-eivj" #new verison 2.1 API no limits
	PATH = os.path.dirname(os.path.abspath(__file__)) + '/'
	OUTFILE = 'currentdata.csv'
	download_save(DOMAIN, APP_TOKEN, DATASET_ID2, PATH, OUTFILE)

if __name__ == '__main__':
	main()