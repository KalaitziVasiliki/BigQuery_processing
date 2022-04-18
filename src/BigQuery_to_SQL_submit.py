'''
As a Product owner I’d like a list of all users with the timestamp of their first session and their time to convert
------------------------------------------------------------------------------------------------------------------------------------------------------
2. A Python program that will utilize the Python Big Query API to submit a SQL query needed to download needed information for the second story.

RUN USING THE FOLLOWING COMMAND:
python BigQuery_to_SQL_submit.py --opt1 [date_arghment_in YYYYMMDD_format]
'''
#--------------------------------------------------------------------------------------------#
# Assumption: Time to convert can be calculated as the sum of all timeOnSite values per user #
#--------------------------------------------------------------------------------------------#

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

#----------------------
#Read configuration file -- Please set the path you are using
sys.path.insert(1, '/BigQuery_processing/configuration/')
import configuration as conf
#----------------------
		
def big_query_to_df(opt1_value):
	client = bigquery.Client()
	query = conf.query2+ opt1_value + conf.query_suffix2
	#query = """SELECT fullVisitorId AS Customer_Id, visitStartTime AS First_Session_Timestamp,	Total_Conv_Time	FROM (	SELECT fullVisitorId,visitStartTime,SUM(totals.timeOnSite) OVER (PARTITION BY fullVisitorId) AS Total_Conv_Time,ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY visitStartTime ASC) rn FROM 	`bigquery-public-data.google_analytics_sample.ga_sessions_"""+ opt1_value +"""` ) WHERE	rn=1;"""
	print('Starting API dataset loading in a dataframe')		
	df = client.query(query).to_dataframe()
	return df	
	
def df_to_csv(df):
		print('Starting dataset export')
		#df.to_csv('/BigQuery_processing/datasets/extracted_data.csv', index=False,header=True)
		df.to_csv(conf.export_path, index=False,header=True)
		print('csv file generated')	

	
def arguments_validation(opt1_value):
	if (args.opt1 is None):
		print('Incoming date argument is not set! Please try again...')
		opt1_value=''
		sys.exit(0)
	else:
		opt1_value=args.opt1
		print('Loading Big Query dataset for date:', opt1_value )
	return opt1_value

	
if __name__ == '__main__':
	start = time.time()
	parser = argparse.ArgumentParser()
	parser.add_argument("--opt1", type=str)	
	args = parser.parse_args()
	opt1_value=arguments_validation(args.opt1)

	#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] can be downoaded and used for running the project, please configure the path used from your side 
	os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = conf.os_environ
	
	#big_query_to_csv(opt1_value) #df = pd.DataFrame()
	df= big_query_to_df(opt1_value)
	print (df)
	df_to_csv(df)
	
	scriptDuration = time.time() - start
	print('\nProcess ended and lasted ', (scriptDuration), 'seconds')
	
	sys.exit(0)
