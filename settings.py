import os
import json

# Set the REST service port
SERVICE_PORT = 5000

# Set the REST service port
HOST = '127.0.0.1'

# Enable/Disable authentication
ENABLE_AUTH = False

# Set the authentication username
USERNAME = 'set_username'

# Set the authentication password
PASSWORD = 'set_password'

# Set the MONGODB URI
MONGO_URI = 'mongodb://localhost:27017/your_db_name'

# Construct the Domain
DOMAIN = {}
schemas = [schema for schema in os.listdir(path='schemas') if schema.endswith('.schema')]
for schema in schemas:
    with open('schemas/' + schema) as schema_file:
        DOMAIN[schema.replace('.schema', '')] = {}
        DOMAIN[schema.replace('.schema', '')]["schema"] = json.load(schema_file)
    schema_file.close()

# Set Resource Methods (Allowed values: GET, POST, DELETE)
#RESOURCE_METHODS = ['GET', 'POST', 'DELETE'] # Uncomment to allow everything
RESOURCE_METHODS = ['GET']

# Set Item Methods (Allowed values: GET, POST, PUT and DELETE)
#ITEM_METHODS = ['GET', 'POST', 'PUT', 'DELETE'] # Uncomment to allow everything
ITEM_METHODS = ['GET']

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