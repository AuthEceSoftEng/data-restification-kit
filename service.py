import os
from eve import Eve
from eve.auth import BasicAuth
from settings import SERVICE_PORT, HOST, ENABLE_AUTH, USERNAME, PASSWORD

class MyBasicAuth(BasicAuth):
    def check_auth(self, username, password, allowed_roles, resource, method):
        return username == USERNAME and password == PASSWORD

if(ENABLE_AUTH):
    app = Eve(auth = MyBasicAuth)
else:
    app = Eve()

if __name__ == '__main__':
    app.run(host=HOST, port=SERVICE_PORT)