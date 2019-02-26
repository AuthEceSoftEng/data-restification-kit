import requests
import json

import numpy as np

class dataImporter:
    """
    This class implements a data importer responsible for importing the data into a MongoDB.
    """
    def __init__(self, dH, keys_to_exclude = []):
        """
        Initializes this data importer. The initialization requires a data handler instance.
        
        :param dH: the data handler instance
        :param keys_to_exclude: a list containing the keys which are to be excluded in the import process
        """
        self.dH = dH
        self.keys_to_exclude = keys_to_exclude

    def import_data(self, url, sep):
        """
        Imports the data into the MongoDB
        
        :param url: the endpoint that accepts the data
        """
        if(not url.endswith('/')):
            url = url + '/'
        
        data = self.dH.read_data("all", sep)
        
        docs = []
        for (i, row) in data.iterrows():
            info = {}
            for key in row.keys():
                if (key not in self.keys_to_exclude):
                    modified_key = key.replace(' ', '_')
                    # numpy.int64 is not json serializable
                    info[modified_key] = int(row[key]) if isinstance(row[key], np.int64) else row[key]
            docs.append(info)
        
        r = requests.post(url + self.dH.dataset_name,
                          headers = {
                             'Content-Type': 'application/json',
                             'Accept': 'application/json'
                        }, data = json.dumps(docs))
                
        response = r.json()
        if(response['_status'] == 'OK'):
            if(len(docs) > 5):
                return {"message": "SUCCESSFUL IMPORT", "data_sample": docs[0:5]}
            else:
                return {"message": "SUCCESSFUL IMPORT", "data_sample": docs[0:len(docs)]}
        else:
            res = {"error": "Data import failed"}
            if("_items" in response):
                for (i,item) in enumerate(response["_items"]):
                    if(item['_status'] != 'OK'):
                        res["Item_" + str(i)] = item
            else:
                res["message"] = "The provided file contains no data"
            return res