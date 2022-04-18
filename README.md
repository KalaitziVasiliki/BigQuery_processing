# BigQuery_processing

Goal
We need to report the following metrics of the data in the Big Query tables:

➔ The ecommerce Conversion rate for the day X. Two dimensions : UserType (whether the session refers
to a New or Returning user) and Platform (whether the session occurred into desktop; ie Web
or Mobile)
➔ The list of all users with the timestamp of their first session and their time to convert

-----------------------------------------------

1. A Python program that will receive an input date as argument, will utilize Python Big Query API
to download session rows for this specific date and calculate conversion rate using only Python.
2. A Python program that will utilize the Python Big Query API to submit a SQL query needed to
download needed information for the second story.

-----------------------------------------------

General comments

The sample dataset contains obfuscated Google Analytics 360 data from the Google Merchandise
Store, a real ecommerce store. The Google Merchandise Store sells Google branded merchandise. The
data is typical of what you would see for an ecommerce website. It includes the following kinds of
information:
➔ Traffic source data: information about where website visitors originate. This includes data
about organic traffic, paid search traffic, display traffic, etc.
➔ Content data: information about the behavior of users on the site.
➔ Transactional data: information about the transactions that occur on the website.
