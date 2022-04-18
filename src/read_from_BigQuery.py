'''
As a Product owner I need to know the ecommerce Conversion rate for the day X. I’d like to be
able to breakdown the above over up to two dimensions : UserType (whether the session refers
to a New or Returning user) and Platform (whether the session occurred into desktop; ie Web or Mobile)
------------------------------------------------------------------------------------------------------

1. A Python program that will receive an input date as argument, will utilize Python Big Query API
to download session rows for this specific date and calculate conversion rate using only Python.
'''

from google.cloud import bigquery
import os
import argparse
import sys
import datetime as dt,time
import requests,json
from requests.auth import HTTPBasicAuth
import logging
from pandas.io.json import json_normalize
import pandas as pd

		
def big_query_to_df(opt1_value):
	client = bigquery.Client() 
	query = '''SELECT * FROM `bigquery-public-data.google_analytics_sample.ga_sessions_''' + opt1_value +'''`;'''
	print('Starting API dataset loading in a dataframe')		
	df = client.query(query).to_dataframe()
	return df

		
		
def arguments_validation(opt1_value):
	if (args.opt1 is None):
		print('Incoming date argument is not set! Please try again...')
		opt1_value=''
		sys.exit(0)
	else:
		opt1_value=args.opt1
		print('Loading Big Query dataset for date:', opt1_value )
	return opt1_value

	
	
def conversion_rate_calc(df):
	#UserType (whether the session refers to a New or Returning user) 
	new_users=df['visitId'][df.visitNumber == 1].count()
	returning_users= df['visitId'][df.visitNumber > 1].count()	
	print('Number of new users: ', new_users)
	print('Number of returning users: ',returning_users)
	
	
	#Platform (whether the session occurred into desktop; ie Web or Mobile)'''
	ds = pd.json_normalize(df['device'])
	count_desktop_sessions=ds['browser'][ds.deviceCategory == 'desktop'].count()
	count_mobile_sessions=ds['browser'][ds.deviceCategory == 'mobile'].count()
	print ('Number of desktop sessions: ', count_desktop_sessions)
	print ('Number of mobile sessions: ', count_mobile_sessions)

	print ('----------------------------------------------------')


	
if __name__ == '__main__':
	start = time.time()
	parser = argparse.ArgumentParser()
	parser.add_argument("--opt1", type=str)	
	args = parser.parse_args()
	opt1_value=arguments_validation(args.opt1)

	#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] can be downoaded and used for running the project, please configure the path used from your side 
	os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "BigQuery_processing/blob/main/tensile-topic-298811-7062d73da8fd.json" 
	
	#big_query_to_csv(opt1_value) #df = pd.DataFrame()
	df= big_query_to_df(opt1_value)
	conversion_rate_calc(df)
	
	scriptDuration = time.time() - start
	print('\nProcess ended and lasted ', (scriptDuration), 'seconds')
	
	sys.exit(0)
