import os
import subprocess
from eve import Eve
from eve.auth import BasicAuth
from flask import Response
from properties import import_service_auth, import_service_username, import_service_password, \
                       import_service_host, import_service_port
from flask_cors import CORS, cross_origin

class MyBasicAuth(BasicAuth):
    def check_auth(self, username, password, allowed_roles, resource, method):
        return username == USERNAME and password == PASSWORD

if(import_service_auth):
    app = Eve(auth = MyBasicAuth, settings="ImportSettings.py")
else:
    app = Eve(settings="ImportSettings.py")
CORS(app, support_credentials = True)

if __name__ == '__main__':
    app.run(host=import_service_host, port=import_service_port)