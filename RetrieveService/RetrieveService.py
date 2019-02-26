import os
import subprocess
import psutil
import json
from eve import Eve
from eve.auth import BasicAuth
from properties import retrieve_service_auth, retrieve_service_username, retrieve_service_password, \
                       retrieve_service_host, retrieve_service_port

class MyBasicAuth(BasicAuth):
    def check_auth(self, username, password, allowed_roles, resource, method):
        return username == retrieve_service_username and password == retrieve_service_password

if(retrieve_service_auth):
    app = Eve(auth = MyBasicAuth, settings="RetrieveSettings.py")
else:
    app = Eve(settings="RetrieveSettings.py")


if __name__ == '__main__':
    app.run(host = retrieve_service_host, port = retrieve_service_port)