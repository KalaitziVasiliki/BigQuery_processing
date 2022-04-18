#----------------------
#read_from_BigQuery configurations

query1= '''SELECT * FROM `bigquery-public-data.google_analytics_sample.ga_sessions_'''
query_suffix1='''`;'''

#----------------------
#BigQuery_to_SQL_submit configurations

query2= """SELECT fullVisitorId AS Customer_Id, visitStartTime AS First_Session_Timestamp,Total_Conv_Time	FROM (SELECT fullVisitorId,visitStartTime,SUM(totals.timeOnSite) OVER (PARTITION BY fullVisitorId) AS Total_Conv_Time,ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY visitStartTime ASC) rn FROM	`bigquery-public-data.google_analytics_sample.ga_sessions_"""
query_suffix2="""` ) WHERE rn=1;"""

export_path='/BigQuery_processing/datasets/extracted_data.csv'

#----------------------
#General configurations

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] can be downoaded and used for running the project, please configure the path used from your side 
os_environ = "/BigQuery_processing/configuration/tensile-topic-298811-7062d73da8fd.json" 
