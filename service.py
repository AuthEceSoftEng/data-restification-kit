import pandas as pd
import numpy as np
import json
import subprocess
import psutil
import os
from flask import Flask, Response, request
from utilities.datasetsHandler import datasetsHandler
from utilities.dataImporter import dataImporter

def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()

app = Flask(__name__)

@app.route('/api/startImporter')
def start():
    
    global proc
    try:
        proc = subprocess.Popen(['C:\Python34\python.exe', 'ImportService' + os.sep + 'ImportService.py'], shell=True)
        return Response(json.dumps({"message": "Service started successfully"}, indent=3), mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({"message": "Service could not start", "exception": e}, indent=3), mimetype="application/json")
#     try:
#         proc.wait(timeout=1000)
#     except subprocess.TimeoutExpired:
#         kill(proc.pid)
#         return Response(json.dumps({"message": "Service Stopped"}, indent=3), mimetype="application/json")

@app.route('/api/checkImporter')
def check():
    
    if(('proc' in globals()) and (proc is not None)):
        return Response(json.dumps({"message": "The import service is running. pid: " + str(proc.pid)}, indent=3), mimetype="application/json")
    else:
        return Response(json.dumps({"message": "The import service is not running"}, indent=3), mimetype="application/json")


@app.route('/api/stopImporter')
def stop():
    kill(proc.pid)
    global proc
    proc = None
    
    return Response(json.dumps({"message": "Service Stopped"}, indent=3), mimetype="application/json")

@app.route('/api/getSchemaTypes')
def getSchemaTypes():
    
    return Response(json.dumps({"types": [val for val in data['label'].values] }, indent=3), mimetype="application/json")

@app.route('/api/registerDataset', methods = ["POST"])
def validateDataset():
    
    print(request.files)
    dataset_file = request.files["dataFile"]
    print(type(request.files["dataFile"]))
    print(dataset_file.filename)
    #r = pd.read_csv(dataset_file)
    dH = datasetsHandler('michalis32', dataset_file)
    
    val = dH.validate('all', ',')
    if(val == True):
        info = {"schema": dH.schema}
        with open('schemas/' + dH.dataset_name + '.schema', 'w') as schema_file:
            json.dump(dH.schema, schema_file, indent=3, sort_keys=True)
        res = json.dumps(info, indent=3)
        
        return Response(res, status = 200, mimetype="application/json")
    else:
        res = json.dumps(val, indent=3)
        
        return Response(res, status = 400, mimetype="application/json")
    
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