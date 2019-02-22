import os
import json
from properties import mongo_uri

# Set the MONGODB URI
MONGO_URI = mongo_uri

# Construct the Domain
DOMAIN = {}
schemas = [schema for schema in os.listdir(path='schemas') if schema.endswith('.schema')]
for schema in schemas:
    with open('schemas/' + schema) as schema_file:
        DOMAIN[schema.replace('.schema', '')] = {}
        DOMAIN[schema.replace('.schema', '')]["schema"] = json.load(schema_file)
    schema_file.close()

# Set Resource Methods (Allowed values: GET, POST, DELETE)
RESOURCE_METHODS = ['GET', 'POST', 'DELETE']

# Set Item Methods (Allowed values: GET, PATCH, PUT and DELETE)
ITEM_METHODS = ['GET', 'PATCH', 'PUT', 'DELETE']

# Set the value in seconds for caching (applicable at GET requests)
CACHE_CONTROL = 'max-age=20'

# Set the url prefix (Comment out for no prefix)
URL_PREFIX = 'api'

# Set the api version (When no prefix set it should be left blank)
API_VERSION = 'v1'

# Set the upper limit for max_results per page
PAGINATION_LIMIT = 100

# Set the default pagination
PAGINATION_DEFAULT = 50