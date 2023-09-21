# DBT-Practice-Yelp-Data

## The purpose of this project is to familiarize myself with new data engineering technologies, namely dbt in this case. 
## This pipeline works as follows:


![image](https://github.com/umergh7/DBT-Practice-Yelp-Data/assets/117035545/0638def2-1418-4eeb-acdb-68db4a9b6016)



1. Extract data from yelp via an API call and convert to a csv file
2. That csv file saves onto the 'seeds' folder on my dbt project and creates a table on GBQ
3. Within my models folder, I transformed the original table to construct useful fact and dimension tables
4. The data is now ready in GBQ and can be used for further BI analysis
