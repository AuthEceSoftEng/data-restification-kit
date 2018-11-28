import os
import json
import numpy as np
import pandas as pd
from builtins import str

class datasetsHandler:
    """
    This class implements a data handler responsible for manipulating the datasets which are to be imported.
    """
    def __init__(self, datasets_folder):
        """
        Initializes this datasets handler. The initialization requires the folder which contains the datasets.
        
        :param datasets_folder: the path of the datasets folder
        """
        self.datasets_folder = datasets_folder
        
        # Get the different datasets which are to be imported. Each folder corresponds to a different dataset
        self.datasets_to_import = [name for name in os.listdir(datasets_folder) if os.path.isdir(os.path.join(datasets_folder, name))]
    
    def get_datasets_files_path(self, dataset, endings = ['.csv']):
        """
        Identifies the files that contain the raw data.
        
        :param dataset: the path of the datasets folder
        :param endings: the supported types of data files (by default .csv files are supported)
        :returns A list containing the paths of raw data files
        """
        
        data_files = []
        for root, dirs, files in os.walk(os.path.join(self.datasets_folder, dataset), topdown=False):
            for name in files:
                for ending in endings:
                    if(name.endswith(ending)):
                        data_files.append('/'.join([root.replace('\\', '/'), name]))
                    
        if(len(data_files) > 0):
            return data_files        
        else:
            return 'No csv file found'

    def read_data(self, file_path, columns = 'all'):
        """
        Identifies the files that contain the raw data.
        
        :param file_path: the path of the raw data file
        :param columns: the columns that are to be read
        :returns A list containing the paths of raw data files
        """
        
        if(columns == 'all'):
            return pd.read_csv(file_path)
        else:
            data = pd.read_csv(file_path)
            return data[columns]
    
    def data_validator(self, df):
        """
        Validates a certain dataset file
        
        :param df: the data frame that contains the data
        :returns True of no validation error exists. In case of error, it returns the errors 
        """
        if(df.isnull().values.any()):
            missing_values_columns = [key for key in df.keys() if df[key].isnull().values.any()]
            
            return 'Validation Error - Missing data in columns: ' + str(missing_values_columns)
        else:
            return True

    def schema_extractor(self, df, write_to_file = False):
        """
        Extracts the data-schema of a given dataset 
        
        :param df: the data frame that contains the data
        :param write_to_file: False/the_schema_file_path
        :returns An object containing the data schema
        """
        schema = {}
        for key in df.keys():
            modified_key = key.replace(' ', '_')
            schema[modified_key] = {}
            if(df[key].dtype == 'object'):
                schema[modified_key]["type"] = 'string'
            else:
                schema[modified_key]["type"] = 'number'
        
        if(write_to_file != False):
            with open(write_to_file, 'w') as schema_file:
                json.dump(schema, schema_file, indent=3, sort_keys=True)
        
        return schema