# -*- coding: utf-8 -*-
"""
Created on Tue Nov  1 13:04:57 2022

@author: umerghani
"""
#Client ID = 'SyDutSLttpFj85STAUNd9Q'

#API Key = 'ymsqw9ahZGeNyYZI2onVfzECIkh-Q6eWqesp3xZpWU3AVLPG6mvUTgyEb3SaKsfmNmdFe0-dpkjIDlpIsmtLQiyUAadvrh4-8dQheCyINTZnE-A6Vfc_KPe9l_tiY3Yx'

#Source Code

#Transaction Search   URL -- 'https://api.yelp.com/v3/transactions/{transaction_type}/search'
#Autocomplete         URL -- 'https://api.yelp.com/v3/autocomplete'

#Categories           URL -- 'https://api.yelp.com/v3/categories'
#Categories Alias     URL -- 'https://api.yelp.com/v3/categories/{alias}'

# Import the modules
import requests
import json
import pandas as pd
from pandas import json_normalize

pd.set_option('display.max_columns', None)
# Define a business ID
#category_alias = 'hotdogs'

# Define API Key, Search Type, and header
MY_API_KEY = ''
BUSINESS_PATH = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': 'bearer %s' % MY_API_KEY}

# Define the Parameters of the search
PARAMETERS = {'location':'Toronto',
              'limit': 50,
              }

# Make a Request to the API, and return results
first_response = requests.get(url=BUSINESS_PATH, 
                        params=PARAMETERS, 
                        headers=HEADERS)

# Convert first_response to a JSON String
data = first_response.json()  
data = json_normalize(data['businesses'], sep='_')
print(data)

data.to_csv('/home/ec2-user/data/yelp_data.csv')
