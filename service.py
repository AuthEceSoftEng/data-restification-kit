import pandas as pd
import numpy as np
import json
import subprocess
import psutil
import os
from flask import Flask, Response, request
from utilities.datasetsHandler import datasetsHandler
from utilities.dataImporter import dataImporter

app = Flask(__name__)
global processes_info
processes_info = {}
processes_info["importer"] = None
processes_info["retriever"] = None
processes_info["importer_available"] = True

def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()

def startImporter():
     
    if(processes_info["importer"] is None):
        try:
            proc = subprocess.Popen(['C:\Python34\python.exe', 'ImportService' + os.sep + 'ImportService.py'], shell=True)
            processes_info["importer"] = proc.pid
             
            return True
        except Exception as e:
            return False
    
    return True

def stopImporter():
    
    if(processes_info["importer"] is not None):
        kill(processes_info["importer"])
    processes_info["importer"] = None
 
    return True

def startRetriever():
     
    if(processes_info["retriever"] is None):
        try:
            proc = subprocess.Popen(['C:\Python34\python.exe', 'RetrieveService' + os.sep + 'RetrieveService.py'], shell=True)
            processes_info["retriever"] = proc.pid
             
            return True
        except Exception as e:
            return False
    
    return True

def stopRetriever():
    
    if(processes_info["retriever"] is not None):
        kill(processes_info["retriever"])
    processes_info["retriever"] = None
 
    return True

# @app.route('/api/startImporter')
# def startImporter():
#     
#     try:
#         proc = subprocess.Popen(['C:\Python34\python.exe', 'ImportService' + os.sep + 'ImportService.py'], shell=True)
#         processes_info["importer"] = proc.pid
#         
#         return Response(json.dumps({"message": "Service started successfully"}, indent=3), mimetype="application/json")
#     except Exception as e:
#         return Response(json.dumps({"message": "Service could not start", "exception": e}, indent=3), mimetype="application/json")
#     try:
#         proc.wait(timeout=1000)
#     except subprocess.TimeoutExpired:
#         kill(proc.pid)
#         return Response(json.dumps({"message": "Service Stopped"}, indent=3), mimetype="application/json")

# @app.route('/api/stopImporter')
# def stopImporter():
#     kill(processes_info["importer"])
#     processes_info["importer"] = None
# 
#     return Response(json.dumps({"message": "Service Stopped"}, indent=3), mimetype="application/json")


@app.route('/api/check')
def check():
    
    info = {}
    if(processes_info["importer"] is not None):
        info["importer"] = "The import service is running. pid: " + str(processes_info["importer"])
    else:
        info["importer"] = "The import service is not running"
    if(processes_info["importer_available"]):
        info["importer_availability"] = "The import service is available"
    else:
        info["importer_availability"] = "The import service is running"
    if(processes_info["retriever"] is not None):
        info["retriever"] = "The retrieve service is running. pid: " + str(processes_info["retriever"])
    else:
        info["retriever"] = "The retrieve service is not running"
        
    return Response(json.dumps(info, indent=3), mimetype="application/json")

@app.route('/api/validateDataset', methods = ["POST"])
def validateDataset():
    
    if(("datasetName" in request.args) and ("separator" in request.args) and ("dataFile" in request.files)):
        dataset_file = request.files["dataFile"]
        
        dH = datasetsHandler(request.args["datasetName"], dataset_file)
        
        val = dH.validate('all', request.args["separator"])
        if(val == True):
            info = {"schema": dH.schema}
            with open('schemas/' + request.args["datasetName"] + '.schema', 'w') as schema_file:
                json.dump(dH.schema, schema_file, indent=3, sort_keys=True)
            res = json.dumps(info, indent=3)
             
            return Response(res, status = 200, mimetype="application/json")
        else:
            res = json.dumps(val, indent=3)
             
            return Response(res, status = 400, mimetype="application/json")
    
    else:
        res = json.dumps({"error": "Missing parameters. You should provide datasetName, separator and dataFile"}, indent=3)
        return Response(res, status = 400, mimetype="application/json")

@app.route('/api/importDataset', methods = ["POST"])
def importDataset():
    
    if(processes_info["importer_available"] == True):
        processes_info["importer_available"] = False
        if(("datasetName" in request.args) and ("separator" in request.args) and ("dataFile" in request.files)):
            dataset_file = request.files["dataFile"]
            if(startImporter()):
                dH = datasetsHandler(request.args["datasetName"], dataset_file)
                dI = dataImporter(dH, request.args["separator"])
                response = dI.import_data('http://localhost:5050/api/v1/', request.args["separator"])
                if('error' in response):
                    stopImporter()
                    res = json.dumps(response, indent=3)
                    processes_info["importer_available"] = True
                    return Response(res, status = 400, mimetype="application/json")
                else:
                    if(stopImporter()):
                        res = json.dumps({"error": "Import successful"}, indent=3)
                        processes_info["importer_available"] = True
                        return Response(res, status = 201, mimetype="application/json")
                    else:
                        res = json.dumps({"error": "Importer stopping procedure failed"}, indent=3)
                        processes_info["importer_available"] = True
                        return Response(res, status = 400, mimetype="application/json")
            else:
                res = json.dumps({"error": "Importer starting procedure failed"}, indent=3)
                processes_info["importer_available"] = True
                return Response(res, status = 400, mimetype="application/json")
        else:
            res = json.dumps({"error": "Missing parameters. You should provide datasetName, separator and dataFile"}, indent=3)
            processes_info["importer_available"] = True
            return Response(res, status = 400, mimetype="application/json")
    else:
        res = json.dumps({"error": "Another import currently in progress. Please try again"}, indent=3)
        return Response(res, status = 400, mimetype="application/json")

@app.route('/api/restartRetriever')
def restartRetriever():
    
    stopRetriever()
    if(startRetriever()):
        res = json.dumps({"message": "Retriever restarted successfully"}, indent=3)
        return Response(res, status = 200, mimetype="application/json")
    else:
        res = json.dumps({"message": "Retriever restart failed"}, indent=3)
        return Response(res, status = 400, mimetype="application/json")

@app.route('/api/getSchemaTypes')
def getSchemaTypes():
    
    return Response(json.dumps({"types": [val for val in data['label'].values] }, indent=3), mimetype="application/json")
    
@app.route('/api/getSchemaProperties')
def getSchemaProperties():
    
    if("type" in request.args):
        type  = request.args["type"]
        
        if(len(data[data['label'].str.lower() == type.lower()]) > 0):
            
            info = data[data['label'].str.lower() == type.lower()]["properties"].iloc[0]
            if(isinstance(info, float)):
                res = json.dumps({"properties": [] }, indent=3)
            else:
                properties = info.split(', ')
                res = json.dumps({"properties": [val.replace('http://schema.org/', '') for val in properties] }, indent=3)
                
            return Response(res, status = 200, mimetype="application/json")
        else:
            res = json.dumps({"error": "Invalid type"}, indent=3)
            return Response(res, status = 400, mimetype="application/json")
    else:
        res = json.dumps({"error": "The URL parameter type must be specified"}, indent=3)
        return Response(res, status = 400, mimetype="application/json")

if __name__ == '__main__':
    # Load schema types
    data = pd.read_csv(os.path.join('utilities', 'schema_data', 'schema-types.csv'))
    
    app.run(host='127.0.0.1', port=5060)